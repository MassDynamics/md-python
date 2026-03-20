# Good Example: Batch Experiment Exploration

## User Request
"I uploaded some mouse tissue phosphoproteomics data — it's called 'uPhos
Tissue DIA-NN PTMs'. Can you find it and tell me what's there?"

## Good Response

```bash
md batch \
  "uploads get uPhos Tissue DIA-NN PTMs --by-name" \
  "datasets list 4e48846a-3ed0-4c80-82dc-23b7430fe8eb" \
  --output /tmp/uphos_info.json
```

Your experiment "uPhos Tissue DIA-NN PTMs" is on the platform:
- Source: DIA-NN tabular, LFQ
- 18 samples across 5 tissue types (heart, eye, brain, brain_HT, brain dilutions)
- Status: completed
- 27 datasets including intensity, pairwise comparisons, and several custom analyses

The pairwise "Compare tissues" dataset ran heart vs eye, heart vs brain,
and heart vs brain_HT comparisons.

## Why This Is Good

- Used `md batch` — one tool call instead of two
- Named the specific experiment details the user cares about
- Didn't dump the full JSON — summarised the key points
- Mentioned the comparisons that were already run (saves the user from
  re-running them)
