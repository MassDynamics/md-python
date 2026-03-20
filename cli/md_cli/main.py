"""
Mass Dynamics CLI v2
====================

Command-line interface for the Mass Dynamics proteomics platform.
Powered by the md-python SDK (v2 API).

Commands:
  md health                        Check API health
  md auth login/status             Authentication
  md uploads get/create/wait       Upload management (v2: experiments → uploads)
  md datasets list/get/wait        Dataset management
  md analysis pairwise/dose/anova  Analysis shortcuts
  md batch "cmd1" "cmd2" ...       Run multiple commands in one call
"""

import json
import os
import sys
import time

import click

from md_python.client import MDClient


# =============================================================================
# HELPERS
# =============================================================================

def get_client():
    """Get an authenticated v2 API client."""
    token = os.environ.get("MD_AUTH_TOKEN")
    base_url = os.environ.get("MD_API_BASE_URL")

    if not token:
        # Try config file
        config_path = os.path.expanduser("~/.md-cli/config.json")
        if os.path.exists(config_path):
            with open(config_path) as f:
                config = json.load(f)
            token = config.get("token")
            base_url = base_url or config.get("base_url")

    if not token:
        click.echo("Error: No API token. Set MD_AUTH_TOKEN or run: md auth login --token YOUR_TOKEN", err=True)
        sys.exit(1)

    return MDClient(api_token=token, base_url=base_url, version="v2")


def output_json(data):
    """Pretty-print JSON to stdout."""
    click.echo(json.dumps(data, indent=2, default=str))


def save_config(token, base_url=None):
    """Save auth config."""
    config_dir = os.path.expanduser("~/.md-cli")
    os.makedirs(config_dir, exist_ok=True)
    config = {
        "token": token,
        "base_url": base_url or "https://dev.massdynamics.com/api",
    }
    with open(os.path.join(config_dir, "config.json"), "w") as f:
        json.dump(config, f, indent=2)
    return config


# =============================================================================
# ROOT
# =============================================================================

@click.group()
@click.version_option(version="2.0.0")
def cli():
    """Mass Dynamics CLI — interact with the MD proteomics platform.

    Powered by the md-python SDK. Uses the v2 API (/uploads, not /experiments).

    Get started:
      md auth login --token YOUR_TOKEN
      md health
      md uploads get "My Experiment" --by-name
    """
    pass


# =============================================================================
# AUTH
# =============================================================================

@cli.group()
def auth():
    """Authentication commands."""
    pass


@auth.command("login")
@click.option("--token", required=True, help="API bearer token (JWT)")
@click.option("--base-url", default=None, help="API base URL")
def auth_login(token, base_url):
    """Save authentication credentials."""
    config = save_config(token=token, base_url=base_url)
    click.echo(f"Token saved. Base URL: {config['base_url']}")


@auth.command("status")
def auth_status():
    """Check if your token is valid."""
    client = get_client()
    result = client.health.check()
    if result.get("status") == "ok":
        click.echo("✓ Authenticated")
    else:
        click.echo(f"✗ {result}")
    output_json(result)


# =============================================================================
# HEALTH
# =============================================================================

@cli.command()
def health():
    """Check API health status."""
    client = get_client()
    output_json(client.health.check())


# =============================================================================
# BATCH
# =============================================================================

@cli.command()
@click.argument("commands", nargs=-1, required=True)
@click.option("--output", "-o", type=click.Path(), default=None,
              help="Save all results to a JSON file")
@click.option("--stop-on-error", is_flag=True,
              help="Stop execution if any command fails")
def batch(commands, output, stop_on_error):
    """Run multiple commands in a single invocation.

    Each argument is a quoted command string. Results are JSON.
    Reuses one authenticated HTTP session for all operations.

    Examples:
      md batch "health" "uploads get <ID>" "datasets list <ID>"
      md batch "health" "uploads get <ID>" --output results.json
    """
    from .batch import run_batch
    results = run_batch(get_client(), commands, stop_on_error)

    out = json.dumps(results, indent=2, default=str)
    if output:
        with open(output, "w") as f:
            f.write(out)
        ok = sum(1 for r in results if r["status"] == "ok")
        err = sum(1 for r in results if r["status"] == "error")
        click.echo(f"✓ {len(results)} commands executed, results saved to {output}")
        click.echo(f"  {ok} succeeded, {err} failed")
    else:
        click.echo(out)


# =============================================================================
# UPLOADS (v2 — replaces "experiments" in v1)
# =============================================================================

@cli.group()
def uploads():
    """Upload management (experiments in v2 API).

    Uploads represent proteomics experiments on the platform.
    The v2 API uses /uploads instead of /experiments.
    """
    pass


@uploads.command("get")
@click.argument("identifier")
@click.option("--by-name", is_flag=True, help="Look up by name instead of UUID")
def uploads_get(identifier, by_name):
    """Get upload details by UUID or name.

    Examples:
      md uploads get 4e48846a-3ed0-4c80-82dc-23b7430fe8eb
      md uploads get "My DIA-NN study" --by-name
    """
    client = get_client()
    if by_name:
        result = client.uploads.get_by_name(identifier)
    else:
        result = client.uploads.get_by_id(identifier)
    output_json(_upload_to_dict(result))


@uploads.command("wait")
@click.argument("upload_id")
@click.option("--timeout", default=600, type=int, help="Max seconds to wait")
@click.option("--interval", default=10, type=int, help="Poll interval seconds")
def uploads_wait(upload_id, timeout, interval):
    """Wait for upload processing to complete."""
    client = get_client()
    result = client.uploads.wait_until_complete(upload_id, poll_s=interval, timeout_s=timeout)
    output_json(_upload_to_dict(result))


# =============================================================================
# DATASETS
# =============================================================================

@cli.group()
def datasets():
    """Dataset and analysis management."""
    pass


@datasets.command("list")
@click.argument("upload_id")
def datasets_list(upload_id):
    """List all datasets for an upload."""
    client = get_client()
    result = client.datasets.list_by_upload(upload_id)
    if result:
        click.echo(f"Found {len(result)} datasets:")
        for ds in result:
            state = getattr(ds, "state", "?")
            dtype = getattr(ds, "type", "?")
            name = getattr(ds, "name", "?")
            did = getattr(ds, "id", "?")
            click.echo(f"  [{state}] {dtype}: {name} ({did})")
    else:
        click.echo("No datasets found")


@datasets.command("find-initial")
@click.argument("upload_id")
def datasets_find_initial(upload_id):
    """Find the initial INTENSITY dataset for an upload."""
    client = get_client()
    ds = client.datasets.find_initial_dataset(upload_id)
    if ds:
        click.echo(f"Initial dataset: {ds.id}")
        output_json(_dataset_to_dict(ds))
    else:
        click.echo("No initial intensity dataset found")


@datasets.command("wait")
@click.argument("upload_id")
@click.argument("dataset_id")
@click.option("--timeout", default=600, type=int)
@click.option("--interval", default=10, type=int)
def datasets_wait(upload_id, dataset_id, timeout, interval):
    """Wait for dataset processing to complete."""
    client = get_client()
    result = client.datasets.wait_until_complete(
        upload_id, dataset_id, poll_s=interval, timeout_s=timeout
    )
    output_json(_dataset_to_dict(result))


# =============================================================================
# ANALYSIS
# =============================================================================

@cli.group()
def analysis():
    """Run analyses with guided parameter input."""
    pass


@analysis.command("pairwise")
@click.option("--input-dataset-id", required=True, help="Intensity dataset UUID")
@click.option("--name", required=True, help="Result name")
@click.option("--sample-metadata", required=True, type=click.Path(exists=True),
              help="CSV with sample_name and condition columns")
@click.option("--condition-column", required=True, help="Column name for conditions")
@click.option("--comparisons", required=True,
              help="Comparison pairs: Treatment:Control,Drug:Vehicle")
def analysis_pairwise(input_dataset_id, name, sample_metadata, condition_column, comparisons):
    """Run pairwise comparison (limma)."""
    import csv
    from md_python.models.metadata import SampleMetadata
    from md_python.models.dataset_builders import PairwiseComparisonDataset

    with open(sample_metadata) as f:
        sm_data = [row for row in csv.reader(f)]

    pairs = [[p.strip() for p in pair.split(":")] for pair in comparisons.split(",")]

    client = get_client()
    dataset_id = PairwiseComparisonDataset(
        input_dataset_ids=[input_dataset_id],
        dataset_name=name,
        sample_metadata=SampleMetadata(data=sm_data),
        condition_column=condition_column,
        condition_comparisons=pairs,
    ).run(client)
    click.echo(f"✓ Pairwise comparison started. Dataset ID: {dataset_id}")


# =============================================================================
# JOBS
# =============================================================================

@cli.command("jobs")
def jobs_list():
    """List available analysis job types."""
    client = get_client()
    result = client.jobs.list()
    if result:
        output_json(result)
    else:
        click.echo("No jobs available")


# =============================================================================
# HELPERS
# =============================================================================

def _upload_to_dict(upload):
    """Convert Upload model to dict for JSON output."""
    if hasattr(upload, "model_dump"):
        return upload.model_dump()
    if hasattr(upload, "__dict__"):
        return {k: str(v) if not isinstance(v, (str, int, float, bool, list, dict, type(None))) else v
                for k, v in upload.__dict__.items() if not k.startswith("_")}
    return str(upload)


def _dataset_to_dict(ds):
    """Convert Dataset model to dict for JSON output."""
    if hasattr(ds, "model_dump"):
        return ds.model_dump()
    if hasattr(ds, "__dict__"):
        return {k: str(v) if not isinstance(v, (str, int, float, bool, list, dict, type(None))) else v
                for k, v in ds.__dict__.items() if not k.startswith("_")}
    return str(ds)


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    cli()


if __name__ == "__main__":
    main()
