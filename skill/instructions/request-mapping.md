# Request Mapping

Map user requests to the correct CLI commands.

| User says | CLI command | Notes |
|---|---|---|
| "Upload my data" | `md uploads create` (not yet in CLI — use Python SDK) | Needs design CSV + files |
| "Find my experiment" | `md uploads get "Name" --by-name` | v2: uploads, not experiments |
| "What datasets do I have?" | `md datasets list <UPLOAD_ID>` | |
| "Find the intensity dataset" | `md datasets find-initial <UPLOAD_ID>` | Needed as input for analyses |
| "Compare two conditions" | `md analysis pairwise ...` | Validate with policy skill first |
| "Run dose-response" | `md analysis dose-response ...` | Validate with policy skill first |
| "What analyses are available?" | `md jobs` | Lists all registered job types |
| "How's my experiment doing?" | `md uploads get <ID>` → check status field | |
| "Is the analysis finished?" | `md datasets wait <UPLOAD_ID> <DS_ID>` | Polls until terminal state |
| "Check the API" | `md health` | Returns {"status": "ok"} |

## Always Prefer Batch

If the user's request maps to 2+ commands, combine them with `md batch`:

```bash
md batch \
  "uploads get <ID>" \
  "datasets list <ID>" \
  "datasets find-initial <ID>" \
  --output results.json
```

This is 24x faster than running commands individually and uses 33% fewer tokens.
