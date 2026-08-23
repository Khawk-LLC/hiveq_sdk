# HiveQ SDK — documentation

The complete reference documentation, versioned per SDK release. Each reference
is a **single plain-markdown file** (`llms.txt`) designed to be loaded in one
read — by a human, Claude, Codex, Kimi, or any other agent. No special tooling.

## Layout

| Path | What it is |
|---|---|
| [`llms.txt`](llms.txt) | **The HiveQ Flow API spec** — every callback, order type, execution algorithm, and result accessor (§0–§16), with the dataset/schema catalog as an appendix (§A.1–§A.4). Always matches the SDK release; the version is stated in its header. |
| [`data_driver/llms.txt`](data_driver/llms.txt) | The data-driver config DSL (`hiveq.driver`) reference (§1–§21) — the data-access tool for HiveQ: transports, caching, subscriptions, and publishing. |
| [`data_api/llms.txt`](data_api/llms.txt) | The **low-level** HiveQ Data API (REST/WebSocket) reference (§0–§8) — the raw endpoints under the driver. Read `data_driver/llms.txt` first: the driver is the recommended path, and this file's §1 itemises what you give up by skipping it. Access is limited to the trusted network and to code running inside the platform. |

A copy of these files ships inside the installed wheel — run `hiveq docs` to
print their installed paths (this repo's `docs/` is the canonical source; the
bundled copy is generated from it at build time).

## How to read

1. Load the whole file in **one** read (~31k tokens for the flow spec) — one
   read is cheap; dozens of fragmented reads of the same content are not.
2. For a targeted question, jump straight to a section: search for a line
   starting `## N.` — prose cross-references use `§N` (`§A.N` for the data
   appendix).
3. Read §0 (hard rules) of the flow spec at least once per session — it's
   short and every other section assumes you've read it.

Available datasets/schemas: the appendix lists them, and `hiveq datasets`
prints the live catalog from HiveQ metadata.
