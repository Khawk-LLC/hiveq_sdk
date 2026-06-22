"""Upload local data files to the HiveQ platform persistent-data store (REST).

Files NEVER touch the client's local disk on the way to the platform — they are
streamed over HTTP to container-service, which stores them in the caller's
per-user persistent-data area. Everything stays on the platform; the executor
reads the files back from there, so a strategy references an uploaded file by
its **relative** path (e.g. ``"userdata/a.csv"``) and the same path resolves on
the platform.

Properties:
- **REST-only:** no local copy, no shared-filesystem assumption — works from any
  remote machine with just an API key.
- **Incremental (rsync-like):** a file is sent only if it's new or its content
  changed, compared by MD5 against the server's current listing.
- **Verified:** the server re-checks each file's MD5 after writing.

Configuration (env / ``~/.hiveq/.env``):
- ``HIVEQ_API_KEY``           — required; identifies the user/org.
- ``HIVEQ_AUTH_URL``          — the single platform host the whole SDK points at
                                (default ``https://staging.hiveq.ai``).
"""
import hashlib
import json
import os
import time
from typing import Iterable, List, Optional, Union

import requests

from hiveq.flow.logger import logger

logger = logger(show_logo=False)

PathLike = Union[str, "os.PathLike[str]"]
_BUFSIZE = 1 << 20  # 1 MiB
# (connect timeout, read timeout) — large files can take a while to stream.
_TIMEOUT = (10, 600)
# Bounded retries: on repeated network timeouts/failures we give up rather than
# hammering the platform forever, and tell the user to check the dashboard.
_MAX_ATTEMPTS = 3
_RETRY_WAIT = 2.0


# --- config -----------------------------------------------------------------
def _container_base_url() -> str:
    """Platform base URL — the single sign-in host (``HIVEQ_AUTH_URL``).

    One configured URL points the whole SDK at the same platform; uploads go to
    that host. Defaults to ``https://staging.hiveq.ai`` when unset.
    """
    from hiveq.flow.config import platform_origin

    return platform_origin().rstrip("/")


def _endpoint(path: str) -> str:
    version = os.environ.get("HIVEQ_CONTAINER_API_VERSION", "v0")
    return f"{_container_base_url()}/api/containers/{version}/{path.lstrip('/')}"


def _auth_headers() -> dict:
    key = os.environ.get("HIVEQ_API_KEY")
    if not key:
        from hiveq.flow.config import HIVEQ_CREDS_FILE
        raise RuntimeError(
            f"HIVEQ_API_KEY not found. Set it in your environment or in "
            f"{HIVEQ_CREDS_FILE}. [Error: API_KEY_MISSING]"
        )
    headers = {"X-API-Key": key}
    org = os.environ.get("HIVEQ_ORG_ID")
    if org:
        headers["X-Org-ID"] = org
    return headers


def _raise_for_status(resp: requests.Response, what: str) -> None:
    if resp.status_code < 400:
        return
    detail = ""
    try:
        body = resp.json()
        detail = body.get("error", {}).get("message") or body.get("message") or ""
    except Exception:
        detail = (resp.text or "")[:300]
    raise RuntimeError(
        f"{what} failed [{resp.status_code}] at {resp.url}: {detail}".rstrip()
    )


def _request(method: str, url: str, what: str, **kwargs) -> requests.Response:
    """Issue a request with bounded retries on network timeouts/failures.

    HTTP errors (4xx/5xx) raise immediately via ``_raise_for_status``. Only
    transient transport errors (timeout / connection) are retried, up to
    ``_MAX_ATTEMPTS``; after that we stop and tell the user to retry later or
    check the dashboard rather than blocking indefinitely.
    """
    last_exc = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            resp = requests.request(method, url, timeout=_TIMEOUT, **kwargs)
            _raise_for_status(resp, what)
            return resp
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt < _MAX_ATTEMPTS:
                logger.warning(
                    f"{what}: network problem ({type(exc).__name__}); "
                    f"retry {attempt}/{_MAX_ATTEMPTS - 1} in {_RETRY_WAIT * attempt:.0f}s"
                )
                time.sleep(_RETRY_WAIT * attempt)
    raise RuntimeError(
        f"{what}: the HiveQ platform is unreachable after {_MAX_ATTEMPTS} attempts "
        f"({type(last_exc).__name__}). Stopping — please try again later or check "
        f"the HiveQ dashboard."
    ) from last_exc


# --- helpers ----------------------------------------------------------------
def _md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_BUFSIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def _iter_files(paths: Iterable[PathLike], base: Optional[str] = None):
    """Yield ``(abs_local_path, relative_name)`` for each file under ``paths``.

    The directory structure is preserved: each argument is anchored to its own
    parent so the thing you point at becomes the top-level entry — uploading
    ``userdata/`` stores files as ``userdata/<subpath>`` no matter what the
    current directory is. Pass ``base`` to anchor every argument to a shared
    root instead. Directories are recursed.
    """
    for p in paths:
        ap = os.path.abspath(os.fspath(p))
        anchor = base or os.path.dirname(ap)
        if os.path.isdir(ap):
            for root, _dirs, files in os.walk(ap):
                for fn in files:
                    fp = os.path.join(root, fn)
                    yield fp, os.path.relpath(fp, anchor)
        elif os.path.isfile(ap):
            yield ap, os.path.relpath(ap, anchor)
        else:
            raise FileNotFoundError(f"upload: no such file or directory: {p}")


def _short(rel: str, width: int = 40) -> str:
    return rel if len(rel) <= width else "…" + rel[-(width - 1):]


def _progress_bar(total: int):
    """A tqdm bar (n/total + elapsed + current file); ``None`` if tqdm absent."""
    try:
        from tqdm import tqdm
    except Exception:
        return None
    return tqdm(
        total=total,
        unit="file",
        dynamic_ncols=True,
        bar_format="Uploading {n_fmt}/{total_fmt} |{bar}| {elapsed}  {desc}",
    )


# --- public API -------------------------------------------------------------
def _list_raw(path: Optional[str] = None) -> dict:
    """Return the raw list payload: ``{"files", "mountPath", "root"}``."""
    params = {"path": path} if path else None
    resp = _request(
        "GET", _endpoint("persistent-data/list"), "list",
        headers=_auth_headers(), params=params,
    )
    return resp.json()["data"]


def list_files(path: Optional[str] = None) -> List[dict]:
    """List the files currently in the caller's persistent-data store.

    Returns a list of ``{"path", "size", "md5", "modifiedAt"}`` dicts. ``path``
    optionally restricts the listing to a subdirectory.
    """
    return _list_raw(path)["files"]


def delete_files(paths: Union[str, Iterable[str]]) -> dict:
    """Delete files (or directories, recursively) from the persistent-data store.

    Paths are the same relative names shown by :func:`list_files` (e.g.
    ``"userdata/algo_signals.csv"`` or a directory like ``"userdata"``).

    Returns ``{"deleted": int, "missing": int, "results": [{"path","status"}]}``.
    """
    if isinstance(paths, (str, bytes, os.PathLike)):
        paths = [paths]
    rels = [os.fspath(p).replace(os.sep, "/") for p in paths]
    resp = _request(
        "POST", _endpoint("persistent-data/delete"), "delete",
        headers=_auth_headers(), json={"paths": rels},
    )
    return resp.json()["data"]


def upload_files(
    paths: Union[PathLike, Iterable[PathLike]],
    base: PathLike = None,
    force: bool = False,
    dry_run: bool = False,
    progress: bool = True,
) -> dict:
    """Stream local data files to the platform persistent-data store over REST.

    Parameters
    ----------
    paths : str | os.PathLike | iterable of those
        Files and/or directories to upload. Directories are recursed.
    base : str | os.PathLike, optional
        Shared root that relative names are computed against. By default each
        argument is anchored to its own parent, so the directory structure is
        preserved (uploading ``userdata/`` stores ``userdata/a.csv``). Reference
        the file in your strategy by that same relative path.
    force : bool, default False
        Re-send every file even if the server already has an identical copy
        (skip the rsync-style MD5 short-circuit).
    dry_run : bool, default False
        Report what would be uploaded/skipped without sending anything.
    progress : bool, default True
        Show a live progress bar (current file, ``n/total``, elapsed time).

    Returns
    -------
    dict : ``{"uploaded": [...], "skipped": [...], "store": "<user store>"}``
    """
    if isinstance(paths, (str, bytes, os.PathLike)):
        paths = [paths]
    # base=None -> each argument is anchored to its own parent (structure
    # preserved); an explicit base anchors all arguments to that shared root.
    base = os.path.abspath(os.fspath(base)) if base else None

    # Enumerate local files (rel names, POSIX), skipping escapes-by-basename.
    local_files = []
    for local, rel in _iter_files(paths, base):
        rel = rel.replace(os.sep, "/")
        if rel.startswith(".."):
            rel = os.path.basename(local)
        local_files.append((local, rel))

    # Incremental: compare against the server's current MD5s.
    server_md5 = {}
    if not force:
        try:
            server_md5 = {f["path"]: f["md5"] for f in list_files()}
        except Exception as exc:  # listing is best-effort; fall back to full send
            logger.debug(f"upload: could not fetch server listing ({exc}); sending all")

    to_send, skipped = [], []
    for local, rel in local_files:
        if not force and server_md5.get(rel) == _md5(local):
            skipped.append(rel)
        else:
            to_send.append((local, rel))

    if dry_run:
        logger.info(
            f"upload (dry-run): {len(to_send)} to upload, {len(skipped)} unchanged"
        )
        return {"uploaded": [r for _, r in to_send], "skipped": skipped,
                "store": _container_base_url(), "dry_run": True}

    bar = _progress_bar(len(to_send)) if progress else None
    uploaded = []
    try:
        for local, rel in to_send:
            if bar is not None:
                bar.set_description_str(_short(rel), refresh=False)
            # Read bytes up front so retries can re-send without a spent handle.
            with open(local, "rb") as fh:
                content = fh.read()
            multipart = {
                "manifest": (None, json.dumps([{"field": "f0", "path": rel}])),
                "f0": (os.path.basename(rel), content, "application/octet-stream"),
            }
            if force:
                multipart["force"] = (None, "true")
            _request(
                "POST", _endpoint("persistent-data/upload"), f"upload {rel}",
                headers=_auth_headers(), files=multipart,
            )
            uploaded.append(rel)
            if bar is not None:
                bar.update(1)
    finally:
        if bar is not None:
            bar.close()

    logger.info(
        f"upload: {len(uploaded)} uploaded, {len(skipped)} unchanged "
        f"-> platform persistent-data store"
    )
    return {"uploaded": uploaded, "skipped": skipped, "store": _container_base_url()}


# --- CLI (`hiveq-data`) -----------------------------------------------------
def _human_size(num: float) -> str:
    """Format a byte count as a compact human-readable string (e.g. 53.3 KB)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num) < 1024 or unit == "TB":
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TB"


def _print_listing(files: List[dict], mount_path: Optional[str] = None) -> None:
    if mount_path:
        print(f"Store (runtime path): {mount_path}")
        print(f"  reference a file by its relative path, or in full as "
              f"{mount_path}/<path>\n")
    if not files:
        print("(empty)")
        return
    width = max((len(f["path"]) for f in files), default=4)
    total = 0
    for f in files:
        total += int(f.get("size", 0))
        print(f"  {f['path']:<{width}}  {_human_size(f.get('size', 0)):>10}  "
              f"{f['md5'][:12]}  {f.get('modifiedAt','')}")
    print(f"\n{len(files)} file(s), {_human_size(total)}")


def main(argv=None) -> int:
    """``hiveq-data`` console entry point — upload/list platform data files.

    Examples::

        hiveq-data -u userdata/                 # upload a directory (incremental)
        hiveq-data -u a.csv b.csv               # specific files
        hiveq-data -u userdata/ --force         # re-send everything
        hiveq-data -u userdata/ --dry-run       # preview, send nothing
        hiveq-data -l                           # list everything in the store
        hiveq-data -l userdata                  # list a subdirectory
        hiveq-data --rm userdata/old.csv        # remove a file
        hiveq-data --rm userdata                # remove a whole subdirectory
    """
    import argparse

    ap = argparse.ArgumentParser(
        prog="hiveq-data",
        description=(
            "Upload and list data files on the HiveQ platform. Uploads stream "
            "over REST to your per-user store (nothing is copied to local disk) "
            "and are incremental by default (only new/changed files, by MD5). "
            "Strategies reference files by their relative path."
        ),
    )
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("-u", "--upload", nargs="+", metavar="PATH",
                       help="files and/or directories to upload")
    group.add_argument("-l", "--list", nargs="?", const="", metavar="SUBPATH",
                       help="list stored files (optionally under a subdirectory)")
    group.add_argument("-d", "--delete", "--rm", nargs="+", metavar="PATH",
                       dest="delete",
                       help="remove files/directories from the store")
    ap.add_argument("--base", default=None,
                    help="root for relative names (default: current directory)")
    ap.add_argument("--force", action="store_true",
                    help="re-upload every file even if unchanged")
    ap.add_argument("--dry-run", action="store_true",
                    help="show what would be uploaded; send nothing")
    ap.add_argument("--quiet", action="store_true", help="suppress the progress bar")
    args = ap.parse_args(argv)

    if args.list is not None:
        data = _list_raw(args.list or None)
        _print_listing(data["files"], data.get("mountPath"))
        return 0

    if args.delete:
        res = delete_files(args.delete)
        print(f"deleted: {res['deleted']}   missing: {res['missing']}")
        for r in res["results"]:
            mark = "-" if r["status"] == "deleted" else "?"
            print(f"  {mark} {r['path']} ({r['status']})")
        return 0

    res = upload_files(args.upload, base=args.base, force=args.force,
                       dry_run=args.dry_run, progress=not args.quiet)
    tag = "would upload" if args.dry_run else "uploaded"
    print(f"{tag}: {len(res['uploaded'])}   unchanged: {len(res['skipped'])}")
    for f in res["uploaded"]:
        print(f"  + {f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
