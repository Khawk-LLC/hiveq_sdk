#!/usr/bin/env python3
"""Run release-validation programs against an explicit HiveQ environment.

This is repository-only QA tooling. It intentionally lives outside ``src/`` so
it is not installed by, or included in, the hiveq-sdk wheel.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PROFILES: dict[str, dict[str, str]] = {
    "localhost": {
        "HIVEQ_AUTH_URL": "http://localhost",
        "HIVEQ_BASE_URL": "http://localhost/api/orchestrator",
        "HIVEQ_DATA_URL": "http://localhost",
    },
    "vm": {
        "HIVEQ_AUTH_URL": "http://vm.hiveq.ai",
        "HIVEQ_BASE_URL": "http://vm.hiveq.ai/api/orchestrator",
        "HIVEQ_DATA_URL": "http://vm.hiveq.ai",
    },
    "staging": {
        "HIVEQ_AUTH_URL": "https://staging.hiveq.ai",
        "HIVEQ_BASE_URL": "https://staging.hiveq.ai/api/orchestrator",
        "HIVEQ_DATA_URL": "https://staging.hiveq.ai",
    },
}

PROFILE_DIR = Path.home() / ".hiveq" / "profiles"


class ProfileError(RuntimeError):
    """A profile cannot be used safely."""


def profile_file(name: str) -> Path:
    return PROFILE_DIR / f"{name}.env"


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return values
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("HIVEQ_"):
            values[key] = value.strip().strip('"').strip("'")
    return values


def write_profile(name: str, api_key: str) -> Path:
    path = profile_file(name)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    values = {**PROFILES[name], "HIVEQ_API_KEY": api_key}
    content = "# Managed by release_validation/hiveq_env.py\n" + "".join(
        f"{key}={value}\n" for key, value in values.items()
    )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    return path


def clean_environment(source: Mapping[str, str] | None = None) -> dict[str, str]:
    source = source or os.environ
    return {key: value for key, value in source.items() if not key.startswith("HIVEQ_")}


def request_status(request: Request, timeout: float = 20.0) -> tuple[int, bytes]:
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()
    except URLError as error:
        raise ProfileError(f"Cannot reach {request.full_url}: {error.reason}") from error


def health_status(name: str) -> int:
    url = f"{PROFILES[name]['HIVEQ_BASE_URL'].rstrip('/')}/health"
    status, _ = request_status(Request(url))
    return status


def key_is_valid(name: str, api_key: str) -> bool:
    auth_url = PROFILES[name]["HIVEQ_AUTH_URL"].rstrip("/")
    payload = json.dumps({"apiKey": api_key}).encode("utf-8")
    request = Request(
        f"{auth_url}/api/auth/verify-api-key",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    status, body = request_status(request)
    if status in (401, 403):
        return False
    try:
        response = json.loads(body)
        data = response.get("data", {})
    except (AttributeError, json.JSONDecodeError) as error:
        raise ProfileError(f"Invalid API-key verification response for profile={name}") from error
    if status == 200 and response.get("success") and data.get("valid"):
        return True

    # Local auth cannot resolve identity from the key alone when the user is in
    # multiple organizations. The key is still valid; prove it against an
    # authenticated orchestrator route, which performs its normal org handling.
    error_code = data.get("error", {}).get("code")
    if status == 409 and error_code == "MULTIPLE_ORGANIZATIONS":
        deployments = Request(
            f"{PROFILES[name]['HIVEQ_BASE_URL'].rstrip('/')}/livesim/deployments",
            headers={"X-API-Key": api_key},
        )
        authenticated_status, _ = request_status(deployments)
        return authenticated_status not in (401, 403)

    if status != 200:
        raise ProfileError(f"API-key verification returned HTTP {status} for profile={name}")
    return False


def browser_login(name: str) -> str:
    # Prevent config.py from importing the unrelated global ~/.hiveq/.env key.
    for key in tuple(os.environ):
        if key.startswith("HIVEQ_"):
            os.environ.pop(key, None)
    os.environ.update(PROFILES[name])
    os.environ["HIVEQ_API_KEY"] = ""

    from hiveq.flow import auth
    from hiveq.flow import config

    path = profile_file(name)
    config.HIVEQ_CREDS_FILE = str(path)
    auth.HIVEQ_CREDS_FILE = str(path)
    key = auth.login()
    write_profile(name, key)
    return key


def resolve_key(name: str, *, force_login: bool = False) -> str:
    stored = read_env_file(profile_file(name)).get("HIVEQ_API_KEY", "")
    if not force_login and stored and key_is_valid(name, stored):
        return stored
    if stored and not force_login:
        print(f"Stored credential is invalid for profile={name}; signing in again.")
    return browser_login(name)


def child_environment(name: str, api_key: str) -> dict[str, str]:
    environment = clean_environment()
    environment.update(PROFILES[name])
    environment["HIVEQ_API_KEY"] = api_key
    environment["HIVEQ_ENV_PROFILE"] = name
    return environment


def print_preflight(name: str, key: str) -> None:
    health = health_status(name)
    if health != 200:
        raise ProfileError(f"Orchestrator health returned HTTP {health} for profile={name}")
    if not key_is_valid(name, key):
        raise ProfileError(f"Credential was rejected by profile={name}")
    print(f"profile={name}")
    print(f"auth_url={PROFILES[name]['HIVEQ_AUTH_URL']}")
    print(f"orchestrator={PROFILES[name]['HIVEQ_BASE_URL']}")
    print("health=ok authentication=ok")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Run one file: python release_validation/hiveq_env.py run vm "
            "release_validation/long_running_t49_long_rollover_buy_hold.py"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("check", "login"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("profile", choices=sorted(PROFILES))
    run_parser = subparsers.add_parser(
        "run",
        help="run one specific validation file (remaining arguments are forwarded)",
    )
    run_parser.add_argument("profile", choices=sorted(PROFILES))
    run_parser.add_argument("--python", default=sys.executable)
    run_parser.add_argument("program", type=Path)
    run_parser.add_argument("arguments", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "login":
            key = resolve_key(args.profile, force_login=True)
            print_preflight(args.profile, key)
            print(f"credential_file={profile_file(args.profile)}")
            return 0

        key = resolve_key(args.profile)
        print_preflight(args.profile, key)
        if args.command == "check":
            print(f"credential_file={profile_file(args.profile)}")
            return 0

        program = args.program.resolve()
        if not program.is_file():
            raise ProfileError(f"Validation program does not exist: {program}")
        command = [args.python, str(program), *args.arguments]
        print(f"running={' '.join(command)}", flush=True)
        return subprocess.call(command, env=child_environment(args.profile, key))
    except (KeyboardInterrupt, ProfileError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
