# Mass Dynamics CLI

Command-line interface for the Mass Dynamics proteomics platform. Powered by
the md-python SDK, using the v2 API.

## Install

```bash
# From the md-python repo
pip install -e ".[cli]"

# Or standalone
pip install md-cli
```

## Quick Start

```bash
# Authenticate
md auth login --token YOUR_TOKEN

# Check connection
md health

# Find your experiment
md uploads get "My DIA-NN study" --by-name

# List datasets
md datasets list <UPLOAD_ID>

# Run multiple operations efficiently
md batch \
  "health" \
  "uploads get <ID>" \
  "datasets list <ID>" \
  --output results.json
```

## Authentication

The CLI stores credentials at `~/.md-cli/config.json`. You can also set
environment variables:

```bash
export MD_AUTH_TOKEN=your_jwt_token
export MD_API_BASE_URL=https://dev.massdynamics.com/api
```

## Command Reference

See [commands.md](commands.md) for the full command reference.

## Common Workflows

See [examples.md](examples.md) for step-by-step workflow examples.

## For AI Agents

This CLI is designed for both human and AI agent use. For Claude Cowork/Code,
install the `md-platform.skill` package which includes this CLI plus
agent-specific guidance for method selection, data validation, and workflow
orchestration.
