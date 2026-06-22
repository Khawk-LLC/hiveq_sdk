"""Browser sign-in that provisions a HiveQ API key (loopback redirect flow).

Users never find, copy, or paste a key. When no ``HIVEQ_API_KEY`` is available,
:func:`login`:

  1. starts a tiny local web server on ``127.0.0.1`` (a random free port),
  2. opens the browser to the HiveQ sign-in page, handing it a one-time
     ``redirect_uri`` that points back at that local server,
  3. the user signs up (new) or in (existing); on success the web app redirects
     the browser to ``http://127.0.0.1:<port>/callback?api_key=…&state=…``,
  4. the local server receives the key, writes it to ``~/.hiveq/.env``, and shuts
     down. The key is in the environment for this process too.

This is the same pattern as ``gcloud auth login`` / ``gh auth login``: the
browser only does auth; the local SDK process is what writes the file (a web page
can't touch your disk). The SDK waits a few seconds for the redirect, no copying.

The sign-in host defaults to ``https://staging.hiveq.ai``. Override it with
``HIVEQ_AUTH_URL`` in ``~/.hiveq/.env`` or the environment when targeting a
different platform. ``HIVEQ_LOGIN_PATH`` overrides the page route (default
``/cli-login``).

Web-app contract
----------------
The sign-in page receives ``?redirect_uri=<loopback>&state=<token>&source=cli``.
After authenticating it must redirect the browser to ``<redirect_uri>`` with the
minted key and the same ``state`` echoed back:
``<redirect_uri>?api_key=<key>&state=<token>`` (or ``?error=<reason>``).
"""
import os
import secrets
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from hiveq.flow.config import HIVEQ_CREDS_FILE
from hiveq.flow.logger import logger

logger = logger()

# Front-end route that handles CLI sign-in. HIVEQ_LOGIN_PATH overrides it.
_DEFAULT_LOGIN_PATH = "/cli-login"


class LoginError(RuntimeError):
    """Raised when the browser sign-in cannot complete."""


def _auth_url() -> str:
    """Sign-in host — ``HIVEQ_AUTH_URL`` env var, defaulting to staging."""
    from hiveq.flow.config import platform_origin
    return platform_origin().rstrip("/")


def _login_path() -> str:
    p = os.environ.get("HIVEQ_LOGIN_PATH") or _DEFAULT_LOGIN_PATH
    return p if p.startswith("/") else "/" + p


def _build_login_url(redirect_uri: str, state: str) -> str:
    base = f"{_auth_url()}{_login_path()}"
    query = urllib.parse.urlencode(
        {"redirect_uri": redirect_uri, "state": state, "source": "cli"}
    )
    sep = "&" if urllib.parse.urlparse(base).query else "?"
    return f"{base}{sep}{query}"


def _write_api_key(api_key: str) -> str:
    """Persist ``HIVEQ_API_KEY`` to ``~/.hiveq/.env``, preserving other entries.

    Upserts the key line (creating the directory/file if needed) and returns the
    creds-file path.
    """
    path = HIVEQ_CREDS_FILE
    os.makedirs(os.path.dirname(path), exist_ok=True)

    lines = []
    if os.path.isfile(path):
        with open(path) as f:
            lines = f.read().splitlines()

    new_line = f"HIVEQ_API_KEY={api_key}"
    replaced = False
    for i, line in enumerate(lines):
        if line.strip().startswith("HIVEQ_API_KEY=") and not line.strip().startswith("#"):
            lines[i] = new_line
            replaced = True
            break
    if not replaced:
        lines.append(new_line)

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    try:
        os.chmod(path, 0o600)  # the key is a secret — keep it private
    except OSError:
        pass
    return path


_SUCCESS_HTML = (
    b"<!doctype html><html><head><meta charset='utf-8'><title>HiveQ</title></head>"
    b"<body style='font-family:-apple-system,Segoe UI,Roboto,sans-serif;text-align:center;padding:48px'>"
    b"<h2>You're signed in to HiveQ.</h2>"
    b"<p>Your API key has been saved. You can close this tab and return to your terminal.</p>"
    b"</body></html>"
)
_ERROR_HTML = (
    b"<!doctype html><html><head><meta charset='utf-8'><title>HiveQ</title></head>"
    b"<body style='font-family:-apple-system,Segoe UI,Roboto,sans-serif;text-align:center;padding:48px'>"
    b"<h2>Sign-in didn't complete.</h2>"
    b"<p>Return to your terminal and try again.</p></body></html>"
)


class _CallbackServer(HTTPServer):
    """Loopback server that captures the one redirect from the sign-in page."""
    expected_state: Optional[str] = None
    result: Optional[dict] = None
    done: threading.Event


class _CallbackHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence default request logging
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return

        params = urllib.parse.parse_qs(parsed.query)

        def first(*keys):
            for k in keys:
                if params.get(k):
                    return params[k][0]
            return None

        server: _CallbackServer = self.server  # type: ignore[assignment]
        error = first("error", "error_description")
        state = first("state")
        api_key = first("api_key", "apiKey", "key", "token", "access_token")

        if server.expected_state and state and state != server.expected_state:
            # State mismatch — likely a stale/forged callback; reject it.
            server.result = {"error": "state_mismatch"}
        elif error:
            server.result = {"error": error}
        elif api_key:
            server.result = {"api_key": api_key}
        else:
            server.result = {"error": "no_api_key_in_callback"}

        ok = "api_key" in server.result
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(_SUCCESS_HTML if ok else _ERROR_HTML)
        server.done.set()


def _print_prompt(url: str) -> None:
    bar = "─" * 64
    print(
        f"\n{bar}\n"
        f"  Opening your browser to sign in to HiveQ…\n\n"
        f"  If it doesn't open, paste this link into your browser:\n\n"
        f"      {url}\n\n"
        f"  Waiting for you to finish signing in…\n"
        f"{bar}\n"
    )


def login(*, timeout: float = 300.0, open_browser: bool = True) -> str:
    """Sign in through the browser and save the resulting HiveQ API key.

    Opens the browser to the sign-in page, waits for the loopback redirect that
    carries the freshly minted key, writes it to ``~/.hiveq/.env``, and returns
    it. Raises :class:`LoginError` on timeout or an error from the web app.
    """
    state = secrets.token_urlsafe(24)

    # Bind to a free loopback port; the web app redirects the browser back here.
    server = _CallbackServer(("127.0.0.1", 0), _CallbackHandler)
    server.expected_state = state
    server.result = None
    server.done = threading.Event()
    port = server.server_address[1]
    redirect_uri = f"http://127.0.0.1:{port}/callback"
    url = _build_login_url(redirect_uri, state)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _print_prompt(url)
        if open_browser:
            try:
                webbrowser.open(url)
            except Exception:
                pass  # link is printed above

        if not server.done.wait(timeout):
            raise LoginError(
                "Timed out waiting for sign-in to complete. Re-run and finish in "
                "your browser, or call hf.login(timeout=...) for a longer window."
            )
    finally:
        server.shutdown()
        server.server_close()

    result = server.result or {}
    if result.get("error"):
        raise LoginError(f"Sign-in failed: {result['error']}.")
    api_key = result.get("api_key")
    if not api_key:
        raise LoginError("Sign-in did not return an API key.")

    path = _write_api_key(api_key)
    os.environ["HIVEQ_API_KEY"] = api_key
    print(f"\n✓ Signed in. Your API key is saved to {path}.\n")
    return api_key


def main() -> int:
    """Console entry point: ``hiveq-login``. Forces a fresh sign-in."""
    try:
        login()
        return 0
    except KeyboardInterrupt:
        print("\nSign-in cancelled.")
        return 1
    except LoginError as e:
        print(f"\n{e}")
        return 1
