## 3. Credentials

A **HiveQ API key** is the only credential required, and the SDK provisions it through browser sign-in — the user never creates, copies, exports, or manages a key by hand. Identity (user, org) is resolved from the key server-side.

How it actually works: the SDK looks for a key in the environment / `~/.hiveq/.env`. If none is found, browser sign-in opens, mints the key, **writes it to `~/.hiveq/.env`, and reuses it on every later run**.

Users have two supported paths:

```bash
hiveq login
```

or simply run a backtest/deploy; the **first** `run_backtest`/`deploy_job` triggers the same sign-in automatically. `hiveq login` is useful before using direct data tools such as `hiveq_data` / the data driver, where there may be no backtest call to trigger sign-in.

### 3.1 First run — the sign-in you will see, and what to do (AI agents: READ THIS)

When you (the AI) run anything that initializes the client with no key saved yet — the first `run_backtest`, or the dedicated sign-in step below — the process prints a banner like this to stdout and then **blocks for up to 5 minutes** waiting for the user to finish signing in:

```
────────────────────────────────────────────────────────────────
  Opening your browser to sign in to HiveQ…

  If it doesn't open, paste this link into your browser:

      https://<host>/cli-login?redirect_uri=http://127.0.0.1:<port>/callback&state=…&source=cli

  Waiting for you to finish signing in…
────────────────────────────────────────────────────────────────
```

A tiny local server on `127.0.0.1:<port>` (same machine as you) receives the redirect, writes the key to `~/.hiveq/.env`, prints `Signed in as <email-or-name>` (or `Signed in to HiveQ` if the host does not return a profile label), and the run continues. **This wait is expected and correct** — it is not a hang, a crash, a missing dependency, or a bug to debug.

**The whole experience the user should have is:** *they run `hiveq login` or start their first backtest, a browser opens, they sign in (or sign up), and their key is saved.* That is it. Do not ask them to copy/export a key or hand-edit files.

**What you MUST do:**
1. **Trigger the sign-in for the user**. For backtests, just run the first backtest (init signs in automatically). For direct data-driver work, use `hiveq login` first.
2. **Tell the user only the human part, in plain words.** A browser window is opening for them to sign in; once they're signed in, they're all set. If the browser didn't open on its own, give them just the bare sign-in link to click:
   > 👉 **A browser will open so you can sign in to HiveQ.** (If it doesn't open, click here: `<url>`) Once you've signed in, you're all set — nothing else to do.
3. **Wait** for it to complete (the process prints `Signed in as <email-or-name>` once the key is written), then continue. Every later run reuses the saved key automatically — sign-in happens **once**.

**What you MUST NOT do** (the real failure modes):
- Treat the wait as a hang, kill the process, and start bisecting / debugging the SDK.
- Conclude the key is "missing" and tell the user to create, paste, or `export` a key, or hand-edit a file.
- Ask the user to run `hiveq-login`, `hf.login()`, manual Python snippets, env exports, or file edits. Use the public `hiveq login` command when an explicit login step is needed.
- Retry the run in a loop or downgrade to a "manual API-key setup" workaround. Signup IS the browser flow — there is no manual key.

> `HIVEQ_AUTH_URL` defaults to `https://staging.hiveq.ai` when not explicitly set. Override it in `~/.hiveq/.env` or the environment to point the SDK at a different platform host (e.g. a local dev instance).

### 3.2 When generating deliverable code or instructions, DO NOT:
- tell the user to `export HIVEQ_API_KEY=...` (or set it in a shell/`.env`/`os.environ` by hand) — the sign-in does this;
- add `hf.login()` to a strategy/backtest script — `run_backtest` already signs in on first use, and you handle first-run sign-in invisibly per §3.1;
- add hedging comments like `# if not already saved via hf.login()` or `# make sure your API key is set`;
- hard-code, print, or ask the user to paste a key;
- surface `hf.login()` / `hiveq-login` to the user as something they should run. Use `hiveq login` only as a one-time terminal setup command, never inside strategy/backtest code.

`hf.login()` / `hiveq-login` is internal plumbing. The user-facing command is `hiveq login`. A finished strategy/backtest script must still be runnable as-is, with **zero** credential code inside it.

---
