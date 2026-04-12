# Common Mistakes

## 1. Running commands individually instead of batch

**Bad:**
```bash
md health
md uploads get <ID>
md datasets list <ID>
```
Three tool calls, ~30k tokens, ~178 seconds.

**Instead:** One `md batch` call, ~21k tokens, ~6 seconds.

## 2. Using v1 experiment terminology

**Bad:** `md experiments get <ID>` (standalone command)

**Better:** `md uploads get <ID>` — the v2 API uses "uploads."
(In batch mode, "experiments get" is backward-compatible and auto-routes.)

## 3. Forgetting --by-name flag

**Bad:** `md uploads get "My Experiment"` — treats the name as a UUID, fails.

**Instead:** `md uploads get "My Experiment" --by-name`

## 4. Dumping raw JSON to the user

**Bad:** Running the command and pasting the entire JSON response.

**Instead:** Summarise the key information (experiment name, status, sample
count, dataset types) and mention where the full data is saved.

## 5. Running analysis without policy validation

**Bad:** Immediately running `md analysis pairwise` without checking the
experimental design.

**Instead:** Hand off to md-analysis-policy first to validate data type,
sample size, design, and method suitability. Then execute.

## 6. Not using --output for batch

**Bad:** `md batch "cmd1" "cmd2"` — prints JSON to stdout, hard to process.

**Instead:** `md batch "cmd1" "cmd2" --output results.json` — saves structured
results for downstream use.
