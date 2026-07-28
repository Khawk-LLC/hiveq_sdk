"""``Probe`` — evidence recording that survives both execution engines.

The problem this solves
-----------------------
A test strategy's callbacks run in one of two places:

* ``--engine=inproc`` (the ``hiveq-flow`` venv): callbacks run in this process,
  so module-level state is visible to the assertions afterwards. But
  ``Run.event_logs()`` returns an empty DataFrame for local runs *by design*
  (``hiveq/flow/runs.py``), so event logs are NOT readable.
* ``--engine=remote`` (the ``hiveq-sdk`` venv): callbacks run in the platform
  executor, so module-level state stays empty here — but ``run.event_logs()``
  IS populated.

Neither channel works in both modes, so a ``Probe`` writes to both and reads
back from whichever one produced data. Test bodies then contain zero
engine-mode branching.

Cost discipline
---------------
Per-event ``add_event_log`` calls would emit thousands of rows and dominate the
run. Instead the probe keeps cheap in-memory counters/samples during the run and
**flushes once** in ``on_stop``. Call ``probe.flush(ctx)`` from every test
strategy's ``on_stop``; ``flush`` is safe to call more than once (``on_stop`` can
fire per session) — ``collect`` keeps the highest-sequence flush.

Usage
-----
::

    from agent_qa.core.probe import Probe
    probe = Probe()                       # module level, next to the strategy

    class T:
        def on_start(self, ctx, event):
            probe.bump("start")
            ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

        def on_bar(self, ctx, event):
            bar = event.data()
            probe.bump("bar")
            probe.sample("bar", symbol=bar.symbol, close=bar.close, ts=event.ts_event)
            if bar.high < bar.low:
                probe.error(f"ohlc inverted on {bar.symbol}")

        def on_stop(self, ctx, event):
            probe.bump("stop")
            probe.flush(ctx)

    # after run.wait(progress=False):
    data = probe.collect(run)
    data.count("bar")        # int
    data.samples("bar")      # list[dict]
    data.errors              # list[str]
"""

from __future__ import annotations

import base64
import json
import re
from typing import Any, Dict, List, Optional

#: ``sub_event_type`` used for every probe row. Chosen so real strategy event
#: logs in the same run are trivially separable.
TAG = "QA_PROBE"

#: Max samples retained per key. Bounds both memory and the flushed payload.
DEFAULT_SAMPLE_LIMIT = 5

#: An ``add_event_log`` message is a string column; keep each row well under any
#: backend limit by chunking the flushed JSON.
_CHUNK_CHARS = 3000

#: Sentinels for the stdout channel. No whitespace, because the executor log
#: strips newlines and joins lines together.
_MARK_OPEN = "@@QAPROBE|"
_MARK_CLOSE = "|QAPROBE@@"
_MARK_RE = re.compile(r"@@QAPROBE\|(\d+)\|([A-Za-z0-9+/=]*)\|QAPROBE@@")


class ProbeData:
    """Read side of a probe — counters, samples and errors from one run."""

    def __init__(
        self,
        counters: Optional[Dict[str, int]] = None,
        samples: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        errors: Optional[List[str]] = None,
        source: str = "empty",
    ) -> None:
        self.counters: Dict[str, int] = counters or {}
        self._samples: Dict[str, List[Dict[str, Any]]] = samples or {}
        self.errors: List[str] = errors or []
        #: "memory" | "event_logs" | "empty" — which channel the data came from.
        self.source = source

    def count(self, key: str) -> int:
        return int(self.counters.get(key, 0))

    def samples(self, key: str) -> List[Dict[str, Any]]:
        return list(self._samples.get(key, []))

    def first(self, key: str) -> Optional[Dict[str, Any]]:
        got = self._samples.get(key)
        return got[0] if got else None

    def keys(self) -> List[str]:
        return sorted(set(self.counters) | set(self._samples))

    @property
    def empty(self) -> bool:
        return not self.counters and not self._samples and not self.errors

    def __repr__(self) -> str:
        return (
            f"ProbeData(source={self.source!r}, counters={self.counters}, "
            f"errors={len(self.errors)})"
        )


class Probe:
    """Write side — instantiate once at module level in a test file."""

    def __init__(self, sample_limit: int = DEFAULT_SAMPLE_LIMIT) -> None:
        self.sample_limit = sample_limit
        self.counters: Dict[str, int] = {}
        self.samples_by_key: Dict[str, List[Dict[str, Any]]] = {}
        self.errors: List[str] = []
        self._flush_seq = 0

    # ------------------------------------------------------------------ write

    def bump(self, key: str, n: int = 1) -> int:
        """Increment a counter. The cheap, per-event call."""
        self.counters[key] = self.counters.get(key, 0) + n
        return self.counters[key]

    def sample(self, key: str, **fields: Any) -> None:
        """Retain up to ``sample_limit`` example payloads for ``key``."""
        bucket = self.samples_by_key.setdefault(key, [])
        if len(bucket) < self.sample_limit:
            bucket.append({k: _jsonable(v) for k, v in fields.items()})

    def error(self, message: str) -> None:
        """Record a contract violation. Any error should fail the test."""
        if len(self.errors) < 50:
            self.errors.append(str(message))

    def observe(self, key: str, **fields: Any) -> None:
        """``bump`` + ``sample`` in one call — the common case."""
        self.bump(key)
        self.sample(key, **fields)

    def flush(self, ctx) -> None:
        """Publish the accumulated state. Call from ``on_stop``.

        Writes to **two** channels, because they have different reliability:

        * **stdout marker (primary)** — captured in the executor log and readable
          via ``run.logs()``. Verified to survive when written from ``on_stop``.
        * **event log (secondary)** — nicer for a human browsing the run, but
          measured on vm.hiveq.ai 2026-07-28: an ``add_event_log`` issued from
          ``on_stop`` is **silently dropped**, while the same call from
          ``on_start``/``on_bar`` persists. So this channel cannot be the one the
          final flush depends on.

        Never raises: a probe failure must not take down the strategy and turn a
        real verdict into an ERROR.
        """
        self._flush_seq += 1
        try:
            payload = json.dumps(
                {
                    "seq": self._flush_seq,
                    "counters": self.counters,
                    "samples": self.samples_by_key,
                    "errors": self.errors,
                },
                default=str,
            )
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            payload = json.dumps({"seq": self._flush_seq,
                                  "errors": [f"probe serialize failed: {exc}"]})

        # base64 with pipe delimiters and no whitespace: the executor log joins
        # lines with newlines stripped, so a line-based or space-delimited format
        # is unparseable, and raw JSON braces collide with surrounding log text.
        try:
            blob = base64.b64encode(payload.encode("utf-8")).decode("ascii")
            print(f"{_MARK_OPEN}{self._flush_seq}|{blob}{_MARK_CLOSE}", flush=True)
        except Exception:  # noqa: BLE001
            pass

        chunks = [payload[i:i + _CHUNK_CHARS]
                  for i in range(0, len(payload), _CHUNK_CHARS)] or [""]
        for idx, chunk in enumerate(chunks):
            try:
                ctx.add_event_log(
                    chunk,
                    sub_event_type=TAG,
                    state_variable={
                        "qa_seq": self._flush_seq,
                        "qa_part": idx,
                        "qa_parts": len(chunks),
                    },
                )
            except Exception:  # noqa: BLE001 - probe must never break the run
                return

    # ------------------------------------------------------------------- read

    def collect(self, run) -> ProbeData:
        """Read evidence back, preferring whichever channel actually has it.

        In-process the local counters are populated (same process); remotely
        they are empty and the event log carries the flushed payload.
        """
        if self.counters or self.samples_by_key or self.errors:
            return ProbeData(
                counters=dict(self.counters),
                samples={k: list(v) for k, v in self.samples_by_key.items()},
                errors=list(self.errors),
                source="memory",
            )
        return collect_from_run(run)

    def reset(self) -> None:
        self.counters.clear()
        self.samples_by_key.clear()
        self.errors.clear()
        self._flush_seq = 0


def collect_from_run(run) -> ProbeData:
    """Reassemble probe state from a finished remote run.

    Tries the executor log first, then event logs. That order is not arbitrary:
    the final flush happens in ``on_stop``, and an ``add_event_log`` issued from
    ``on_stop`` never persists (measured on vm.hiveq.ai 2026-07-28: 60s of
    polling across ``run.event_logs()``, ``hf.event_logs()`` and
    ``hf.get_run(id).event_logs()`` — the row never appears, while the same call
    from ``on_start``/``on_bar`` lands immediately). stdout from ``on_stop`` does
    survive, so it is the only channel that can carry final state.
    """
    from_logs = _collect_from_logs(run)
    if not from_logs.empty:
        return from_logs
    return _collect_from_event_logs(run)


def _collect_from_logs(run) -> ProbeData:
    """Parse ``@@QAPROBE|seq|base64|QAPROBE@@`` markers out of executor stdout.

    Regex over the joined text rather than per line: the executor log strips
    newlines and concatenates output, so line-oriented parsing finds nothing.
    """
    try:
        lines = run.logs() or []
    except Exception:  # noqa: BLE001
        return ProbeData(source="empty")

    text = "\n".join(str(x) for x in lines)
    best_seq, best = -1, None
    for seq_str, blob in _MARK_RE.findall(text):
        try:
            seq = int(seq_str)
            decoded = json.loads(base64.b64decode(blob).decode("utf-8"))
        except (ValueError, TypeError, json.JSONDecodeError):
            continue
        if seq > best_seq:
            best_seq, best = seq, decoded

    if best is None:
        return ProbeData(source="empty")
    return ProbeData(
        counters={k: int(v) for k, v in (best.get("counters") or {}).items()},
        samples=best.get("samples") or {},
        errors=list(best.get("errors") or []),
        source="executor_log",
    )


def _collect_from_event_logs(run) -> ProbeData:
    """Fallback channel. Tolerant of column naming across API versions."""
    try:
        df = run.event_logs()
    except Exception:  # noqa: BLE001 - a missing run is not a probe failure
        return ProbeData(source="empty")

    if df is None or getattr(df, "empty", True):
        return ProbeData(source="empty")

    rows = df.to_dict("records")
    sub_col = _pick(rows[0], ("sub_event_type", "subEventType", "sub_type"))
    msg_col = _pick(rows[0], ("message", "msg", "text", "log_message"))
    if not msg_col:
        return ProbeData(source="empty")

    # Group chunks by flush sequence; keep only the last (highest) flush.
    by_seq: Dict[int, Dict[int, str]] = {}
    for row in rows:
        if sub_col and str(row.get(sub_col) or "") != TAG:
            continue
        seq, part = _seq_and_part(row)
        by_seq.setdefault(seq, {})[part] = str(row.get(msg_col) or "")

    for seq in sorted(by_seq, reverse=True):
        parts = by_seq[seq]
        blob = "".join(parts[i] for i in sorted(parts))
        try:
            decoded = json.loads(blob)
        except (TypeError, ValueError):
            continue
        return ProbeData(
            counters={k: int(v) for k, v in (decoded.get("counters") or {}).items()},
            samples=decoded.get("samples") or {},
            errors=list(decoded.get("errors") or []),
            source="event_logs",
        )

    return ProbeData(source="empty")


def _seq_and_part(row: Dict[str, Any]) -> tuple:
    """Recover (seq, part) from the state_variable blob, however it round-trips."""
    for key in ("state_variable", "state_variables", "stateVariable", "state"):
        raw = row.get(key)
        if raw is None:
            continue
        data = raw
        if isinstance(raw, str):
            try:
                data = json.loads(raw)
            except (TypeError, ValueError):
                continue
        if isinstance(data, dict):
            return int(data.get("qa_seq") or 0), int(data.get("qa_part") or 0)
    return 0, 0


def _pick(row: Dict[str, Any], candidates) -> Optional[str]:
    for name in candidates:
        if name in row:
            return name
    lowered = {k.lower(): k for k in row}
    for name in candidates:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
