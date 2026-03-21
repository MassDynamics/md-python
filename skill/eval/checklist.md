# Platform Skill Quality Checklist

Run these checks before presenting results to the user.

## 1. Used batch for multi-step operations?
If 2+ API calls were needed, were they combined with `md batch`?
→ PASS / FAIL

## 2. Correct API version?
Did you use v2 commands (uploads, not experiments)?
→ PASS / FAIL

## 3. Auth working?
Did the health check return `{"status": "ok"}`? If not, flag the auth issue.
→ PASS / FAIL

## 4. Summarised results?
Did you summarise key findings rather than dumping raw JSON?
→ PASS / FAIL

## 5. Policy checked (for analyses)?
If the user asked to run an analysis, was md-analysis-policy consulted first?
→ PASS / FAIL (N/A if not an analysis request)

## 6. Error handling?
If any command failed (500, 401, timeout), was the error explained clearly
with an actionable next step?
→ PASS / FAIL

## 7. Output saved?
For batch operations, was `--output` used to save results for potential
follow-up processing?
→ PASS / FAIL
