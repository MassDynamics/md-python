# Common Workflows

## 1. Check your connection

```bash
md health
md auth status
```

## 2. Find an experiment and its datasets

```bash
# By name
md uploads get "My DIA-NN Experiment" --by-name

# By ID
md uploads get 4e48846a-3ed0-4c80-82dc-23b7430fe8eb

# List datasets
md datasets list 4e48846a-3ed0-4c80-82dc-23b7430fe8eb

# Find the intensity dataset (needed for analyses)
md datasets find-initial 4e48846a-3ed0-4c80-82dc-23b7430fe8eb
```

## 3. Batch: do it all in one call

```bash
md batch \
  "health" \
  "uploads get 4e48846a-3ed0-4c80-82dc-23b7430fe8eb" \
  "datasets list 4e48846a-3ed0-4c80-82dc-23b7430fe8eb" \
  "datasets find-initial 4e48846a-3ed0-4c80-82dc-23b7430fe8eb" \
  --output experiment_info.json
```

## 4. Run a pairwise comparison

```bash
# Create a design CSV
cat > design.csv << EOF
sample_name,condition
S1,Control
S2,Control
S3,Treatment
S4,Treatment
EOF

# Run limma pairwise
md analysis pairwise \
  --input-dataset-id <INTENSITY_ID> \
  --name "Treatment vs Control" \
  --sample-metadata design.csv \
  --condition-column condition \
  --comparisons "Treatment:Control"

# Wait for it
md datasets wait <UPLOAD_ID> <NEW_DATASET_ID>
```

## 5. Explore available analysis types

```bash
md jobs
```
