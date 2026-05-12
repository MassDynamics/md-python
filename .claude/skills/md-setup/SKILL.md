---
name: md-setup
description: >
  Set up the Mass Dynamics Python client and MCP server for Claude desktop.
  Use this skill when the user wants to install md-python, configure the MCP
  server, or get started with the Mass Dynamics API from Claude.
---

# Mass Dynamics — Setup Skill

Guides the user through installing the `md-python` client and configuring the
MCP server so that Mass Dynamics tools are available in Claude desktop (Chat
and Cowork). Works step by step, running commands when in Claude Code.

---

## Step 1 — Check prerequisites

Run the following to confirm Python and Git are available:

```bash
python3 --version   # needs 3.11 or higher
git --version
```

If Python is below 3.11, tell the user to upgrade before continuing.
If git is missing, tell them to install it from https://git-scm.com.

---

## Step 2 — Get the repo

Clone the repository:

```bash
git clone https://github.com/MassDynamics/md-python.git
cd md-python
```

If the user already has the repo, `cd` into it and skip this step.

---

## Step 3 — Create a virtual environment and install

Always install into a dedicated `.venv` — never into the system Python or any
shared environment.

```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows
```

Then install the package and MCP dependency:

```bash
pip install -e ".[mcp]"
```

The `-e` flag installs in development mode so source changes are picked up
automatically. Verify it worked:

```bash
python -c "import md_python; import mcp; print('OK')"
```

Keep note of the venv's Python path — you'll need it in Step 5:

```bash
which python    # macOS/Linux — copy this output
```

---

## Step 4 — Add the API token

Create a `.env` file in the repo root with the user's Mass Dynamics API token.
Ask the user for their token, then write:

```bash
echo "MD_AUTH_TOKEN=<their_token>" >> .env
echo "MD_API_BASE_URL=https://app.massdynamics.com/api" >> .env
```

Remind them:
- The token is available from their Mass Dynamics account settings.
- `.env` is git-ignored — it will not be committed.

---

## Step 5 — Configure Claude desktop

Detect the config file path based on OS:
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

Read the file if it exists (create it if not), and add or update the
`mass-dynamics` entry under `mcpServers`. Use the **absolute path** to both
`mcp_server.py` and the `.venv` Python interpreter (from Step 3).

Run this snippet **with the venv active** to configure it automatically:

```python
import json, sys
from pathlib import Path

repo = Path.cwd()                          # must be run from inside the repo
server = repo / "mcp_server.py"
python  = repo / ".venv" / "bin" / "python"   # use .venv\Scripts\python.exe on Windows

# Detect config path by OS
import platform
if platform.system() == "Darwin":
    config_path = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
elif platform.system() == "Windows":
    config_path = Path(os.environ["APPDATA"]) / "Claude" / "claude_desktop_config.json"
else:
    config_path = Path.home() / ".config" / "Claude" / "claude_desktop_config.json"

config = {}
if config_path.exists():
    config = json.loads(config_path.read_text())

config.setdefault("mcpServers", {})["mass-dynamics"] = {
    "command": str(python),
    "args": [str(server)],
}

config_path.write_text(json.dumps(config, indent=2))
print(f"Written to {config_path}")
```

---

## Step 6 — Restart Claude desktop

Tell the user to fully quit and reopen Claude desktop. The Mass Dynamics tools
will then be available in both Chat and Cowork.

---

## Step 7 — Verify

Back in Claude desktop, the user can type:

> "Check the Mass Dynamics API health"

Claude will call the `health_check` tool. A response with `"status": "ok"`
confirms everything is working.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ModuleNotFoundError: mcp` | Make sure the venv is active, then re-run `pip install -e ".[mcp]"` |
| Tools not appearing in Claude | Check `claude_desktop_config.json` points to `.venv/bin/python`, not system Python |
| `MD_AUTH_TOKEN` not found | Check `.env` exists in the repo root and the token has no extra spaces |
| Token rejected by API | Generate a fresh token from your Mass Dynamics account settings |
