# Batch Command Usage

Always use `md batch` when you need 2 or more API operations. It runs all
commands in one tool call, reusing a single authenticated HTTP session.

## Why Batch Matters

| Approach | Tokens | Duration | Tool Calls |
|---|---|---|---|
| Individual commands | ~30k | ~178s | 11+ |
| `md batch` | ~21k | ~6s | 1 |

Batch is 24x faster and 33% more token-efficient.

## Syntax

```bash
md batch "command1" "command2" "command3" [--output file.json] [--stop-on-error]
```

Each command is a quoted string containing the subcommand and its arguments.

## Supported Commands in Batch

- `"health"` — health check
- `"auth status"` — auth verification
- `"uploads get <ID>"` — get upload by UUID
- `"uploads get <name> --by-name"` — get upload by name
- `"experiments get <ID>"` — backward compat, routes to uploads
- `"datasets list <UPLOAD_ID>"` — list datasets
- `"datasets get <DS_ID> -e <UPLOAD_ID>"` — get dataset details
- `"datasets find-initial <UPLOAD_ID>"` — find intensity dataset
- `"jobs"` — list analysis types

## Common Patterns

### Explore an experiment
```bash
md batch \
  "uploads get <ID>" \
  "datasets list <ID>" \
  "datasets find-initial <ID>" \
  --output explore.json
```

### Check status of everything
```bash
md batch \
  "health" \
  "auth status" \
  "uploads get <ID>" \
  --output status.json
```

### Save results for processing
Always use `--output` when you need to process results downstream. The JSON
output contains each command's result (or error) as an array of objects:

```json
[
  {"command": "health", "status": "ok", "result": {"status": "ok"}},
  {"command": "datasets list <ID>", "status": "ok", "result": [...]}
]
```
