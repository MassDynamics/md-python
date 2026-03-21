"""
Mass Dynamics CLI
=================

Command-line interface for the Mass Dynamics proteomics platform.

Commands:
  md health                              Check API health
  md auth login/status                   Authentication
  md experiments list/get/create/wait     Experiment management
  md datasets list/get/create/retry/wait  Dataset & analysis management
  md analysis pairwise/dose-response/anova  Analysis shortcuts
  md design infer                        Infer sample design from experiment
  md tables list/headers/download/query  Data access
  md workspaces list/get/create          Workspace management
  md viz volcano/heatmap/pca/...         Visualisations
  md enrichment reactome/string          Pathway analysis
  md jobs list                           List available analysis types

Output format:
  All commands output JSON to stdout by default.
  Use --format ids-only on key commands to get just the resource ID.
"""
import csv
import json
import sys
import time
import click
from pathlib import Path
from .api import MDClient
from .config import save_config, get_config


# =============================================================================
# HELPERS
# =============================================================================

def get_client():
    """Get an authenticated API client."""
    config = get_config()
    if not config.get("token"):
        click.echo("Error: No API token configured.", err=True)
        click.echo("", err=True)
        click.echo("  Option 1 (quick):      md auth login --token YOUR_TOKEN", err=True)
        click.echo("  Option 2 (env var):     export MD_API_TOKEN=your-token-here", err=True)
        click.echo("  Option 3 (persistent):  Add to Claude settings.json:", err=True)
        click.echo('                          { "env": { "MD_API_TOKEN": "..." } }', err=True)
        click.echo("", err=True)
        click.echo("  Generate a token at: MD web app → Settings → API Access", err=True)
        sys.exit(1)
    return MDClient()


def output_result(data, output_format="json"):
    """Output data in the requested format.

    Formats:
      json      - Pretty-printed JSON (default)
      ids-only  - Just the resource ID, for piping into other commands
      table     - Human-readable table (falls back to JSON for complex data)
    """
    if output_format == "ids-only":
        # Extract the most relevant ID field
        if isinstance(data, dict):
            id_val = (data.get("id") or data.get("experiment_id")
                      or data.get("dataset_id") or data.get("workspace_id"))
            if id_val:
                click.echo(id_val)
                return
        elif isinstance(data, list):
            # For lists, output one ID per line
            for item in data:
                if isinstance(item, dict):
                    id_val = (item.get("id") or item.get("experiment_id")
                              or item.get("dataset_id"))
                    if id_val:
                        click.echo(id_val)
            return
        # Fallback to JSON if no ID found
        click.echo(json.dumps(data, indent=2, default=str))
    elif output_format == "table":
        # Simple table for list data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            keys = list(data[0].keys())[:5]  # First 5 columns
            header = "\t".join(keys)
            click.echo(header)
            click.echo("-" * len(header))
            for item in data:
                click.echo("\t".join(str(item.get(k, "")) for k in keys))
        else:
            click.echo(json.dumps(data, indent=2, default=str))
    else:
        click.echo(json.dumps(data, indent=2, default=str))


def output_json(data):
    """Pretty-print JSON to stdout. Legacy helper — delegates to output_result."""
    output_result(data, "json")


def parse_conditions_string(conditions_str, condition_column="condition"):
    """Parse inline conditions string into column-oriented dict.

    Format: "sample1:Control,sample2:Control,sample3:Treatment"
    Returns: {"sample_name": ["sample1", "sample2", "sample3"],
              "condition": ["Control", "Control", "Treatment"]}
    """
    design = {"sample_name": [], condition_column: []}
    for pair in conditions_str.split(","):
        pair = pair.strip()
        if ":" not in pair:
            click.echo(f"Error: Invalid condition pair '{pair}'. Expected 'sample_name:condition'.", err=True)
            sys.exit(1)
        sample, cond = pair.split(":", 1)
        design["sample_name"].append(sample.strip())
        design[condition_column].append(cond.strip())
    return design


def resolve_design(design_csv, conditions, condition_column="condition"):
    """Resolve experiment design from either --design-csv or --conditions.

    Returns column-oriented dict for the API.
    """
    if design_csv:
        return read_csv_as_dict(design_csv)
    elif conditions:
        return parse_conditions_string(conditions, condition_column)
    else:
        click.echo("Error: Provide either --design-csv or --conditions.", err=True)
        click.echo("  --design-csv design.csv                                  (CSV file)", err=True)
        click.echo("  --conditions 'sample1:Control,sample2:Treatment,...'      (inline)", err=True)
        click.echo("", err=True)
        click.echo("  Tip: Run 'md design infer <EXP_ID>' to discover sample names.", err=True)
        sys.exit(1)


def read_csv_as_arrays(path):
    """Read a CSV file into array-of-arrays format (with header row).

    The MD API expects experiment_design and sample_metadata as:
        [["col1", "col2"], ["val1", "val2"], ...]
    """
    with open(path) as f:
        return [row for row in csv.reader(f)]


def read_csv_as_dict(path):
    """Read a CSV file into column-oriented dict format.

    Used for analysis job params where the API expects:
        {"sample_name": ["s1", "s2"], "condition": ["c1", "c2"]}
    """
    with open(path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return {}
    result = {col: [] for col in rows[0].keys()}
    for row in rows:
        for col, val in row.items():
            result[col].append(val)
    return result


def parse_comparisons(pairs_str):
    """Parse comparison pairs from CLI string.

    Format: "Treatment:Control,Drug:Vehicle"
    Returns: {"condition_comparison_pairs": [["Treatment","Control"], ["Drug","Vehicle"]]}
    """
    pairs = []
    for pair in pairs_str.split(","):
        parts = pair.strip().split(":")
        if len(parts) == 2:
            pairs.append([parts[0].strip(), parts[1].strip()])
    return {"condition_comparison_pairs": pairs}


def wait_for_status(client, get_fn, id_val, terminal_states, timeout, interval, label=""):
    """Generic polling loop."""
    start = time.time()
    last_status = "unknown"
    while time.time() - start < timeout:
        result = get_fn(id_val)
        status = result.get("status") or result.get("state", "unknown")
        last_status = status
        elapsed = int(time.time() - start)
        click.echo(f"  [{label or id_val}] {status} ({elapsed}s)")
        if status.lower() in terminal_states:
            output_json(result)
            return result
        time.sleep(interval)
    click.echo(f"Timeout after {timeout}s. Last status: {last_status}", err=True)
    sys.exit(1)


# =============================================================================
# ROOT
# =============================================================================

@click.group()
@click.version_option(version="1.0.0")
def cli():
    """Mass Dynamics CLI - interact with the MD proteomics platform.

    Manage experiments, run analyses, download results, and generate
    visualisations from the command line.

    Get started:
      md auth login --token YOUR_TOKEN
      md health
      md experiments list
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
@click.option("--base-url", default=None, help="API base URL (default: https://dev.massdynamics.com/api)")
def auth_login(token, base_url):
    """Save authentication credentials.

    Generate a token at: https://dev.massdynamics.com/api/doc
    Or via: POST /users/personal_access_tokens (requires browser session)
    """
    config = save_config(token=token, base_url=base_url)
    click.echo(f"Token saved. Base URL: {config['base_url']}")


@auth.command("status")
def auth_status():
    """Check if your token is valid."""
    client = get_client()
    result = client.auth_status()
    if result.get("authenticated"):
        click.echo("✓ Authenticated")
    else:
        click.echo(f"✗ Not authenticated: {result.get('error')}")
    output_json(result)


# =============================================================================
# HEALTH
# =============================================================================

@cli.command()
def health():
    """Check API health status."""
    client = get_client()
    output_json(client.health())


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
    """Run multiple MD commands in a single invocation.

    Each argument is a full command string (quoted). Results are printed
    as a JSON array, one entry per command. With --output, results are
    saved to a file instead.

    This is much more efficient than running separate CLI calls because
    it reuses a single authenticated HTTP session and avoids per-command
    shell/process overhead.

    Examples:
      md batch "health" "auth status" "experiments get <ID>"

      md batch \\
        "health" \\
        "experiments get 4e48846a-3ed0-4c80-82dc-23b7430fe8eb" \\
        "datasets list 4e48846a-3ed0-4c80-82dc-23b7430fe8eb" \\
        --output results.json

      md batch \\
        "datasets get <DS_ID> -e <EXP_ID>" \\
        "tables headers <EXP_ID> <DS_ID> proteins" \\
        --stop-on-error
    """
    import shlex
    from io import StringIO
    from contextlib import redirect_stdout, redirect_stderr

    results = []
    client_instance = get_client()

    for cmd_str in commands:
        entry = {"command": cmd_str, "status": "ok", "result": None, "error": None}
        try:
            args = shlex.split(cmd_str)
            if not args:
                entry["status"] = "error"
                entry["error"] = "Empty command"
                results.append(entry)
                continue

            # Execute the command by dispatching to the appropriate handler
            result = _dispatch_batch_command(client_instance, args)
            entry["result"] = result
        except Exception as e:
            entry["status"] = "error"
            entry["error"] = str(e)
            if stop_on_error:
                results.append(entry)
                break

        results.append(entry)

    out = json.dumps(results, indent=2, default=str)
    if output:
        with open(output, "w") as f:
            f.write(out)
        click.echo(f"✓ {len(results)} commands executed, results saved to {output}")
        # Summary line
        ok = sum(1 for r in results if r["status"] == "ok")
        err = sum(1 for r in results if r["status"] == "error")
        click.echo(f"  {ok} succeeded, {err} failed")
    else:
        click.echo(out)


def _dispatch_batch_command(client, args):
    """Route a batch command to the appropriate API call.

    Supports a subset of the full CLI commands — the ones most commonly
    used in multi-step workflows.
    """
    cmd = args[0].lower() if args else ""

    if cmd == "health":
        return client.health()

    elif cmd == "auth" and len(args) > 1 and args[1] == "status":
        return client.auth_status()

    elif cmd == "experiments" and len(args) > 1:
        subcmd = args[1]
        if subcmd == "get" and len(args) > 2:
            identifier = args[2]
            if "--by-name" in args:
                return client.get_experiment_by_name(identifier)
            return client.get_experiment(identifier)
        elif subcmd == "list":
            scope = args[2] if len(args) > 2 else "mine"
            return client.list_experiments(scope)

    elif cmd == "datasets" and len(args) > 1:
        subcmd = args[1]
        if subcmd == "list" and len(args) > 2:
            return client.list_datasets(args[2])
        elif subcmd == "get" and len(args) > 2:
            ds_id = args[2]
            exp_id = None
            for i, a in enumerate(args):
                if a in ("-e", "--experiment-id") and i + 1 < len(args):
                    exp_id = args[i + 1]
            return client.get_dataset(ds_id, experiment_id=exp_id)

    elif cmd == "tables" and len(args) > 1:
        subcmd = args[1]
        if subcmd == "headers" and len(args) > 4:
            return client.get_table_headers(args[2], args[3], args[4])
        elif subcmd == "query" and len(args) > 4:
            # Find --sql value
            sql = None
            for i, a in enumerate(args):
                if a == "--sql" and i + 1 < len(args):
                    sql = args[i + 1]
            if not sql:
                # Maybe the SQL is the 5th positional arg
                sql = args[4] if len(args) > 4 else None
            if sql:
                return client.query_table(args[2], args[3], args[4].split()[0] if " " in args[4] else args[4], sql)
            raise Exception("Missing --sql argument for tables query")

    elif cmd == "viz" and len(args) > 1:
        raise Exception("Visualisation commands are not yet supported in batch mode")

    elif cmd == "enrichment" and len(args) > 1:
        raise Exception("Enrichment commands are not yet supported in batch mode")

    raise Exception(f"Unrecognized batch command: {' '.join(args)}")


# =============================================================================
# EXPERIMENTS
# =============================================================================

@cli.group()
def experiments():
    """Experiment management.

    Experiments are the top-level container in MD. They hold raw data files,
    sample metadata, and one or more analysis datasets.

    Lifecycle: create → upload files → start workflow → processing → completed

    Sources: diann_tabular, diann_raw, maxquant, spectronaut, generic_format
    Labelling: lfq (label-free), tmt (tandem mass tag)
    Species: human, mouse, yeast, chinese_hamster
    """
    pass


@experiments.command("list")
@click.option("--scope", default="mine",
              type=click.Choice(["mine", "shared", "organisation", "papers"]),
              help="Which experiments to list (default: mine)")
def experiments_list(scope):
    """List experiments.

    Scopes:
      mine          - Your experiments
      shared        - Experiments shared with you
      organisation  - All experiments in your org
      papers        - Published paper experiments

    Note: List requires session cookie auth. If you only have a bearer token,
    use 'md experiments get <name> --by-name' to find specific experiments.
    """
    client = get_client()
    try:
        result = client.list_experiments(scope)
    except Exception as e:
        click.echo(f"Error listing experiments: {e}", err=True)
        click.echo("Note: Listing requires session cookie auth. Use 'md experiments get <name> --by-name' instead.", err=True)
        sys.exit(1)
    if isinstance(result, list):
        click.echo(f"Found {len(result)} experiments:")
        for exp in result:
            status = exp.get("status", "?")
            name = exp.get("name", "?")
            eid = exp.get("id", "?")
            click.echo(f"  [{status}] {name} ({eid})")
    else:
        output_json(result)


@experiments.command("get")
@click.argument("identifier")
@click.option("--by-name", is_flag=True, help="Look up by name instead of UUID")
def experiments_get(identifier, by_name):
    """Get experiment details by UUID or name.

    Examples:
      md experiments get 5025a4ac-8818-4072-8a58-fd5dc88b0f71
      md experiments get "My DIA-NN study" --by-name
    """
    client = get_client()
    if by_name:
        result = client.get_experiment_by_name(identifier)
    else:
        result = client.get_experiment(identifier)
    output_json(result)


@experiments.command("create")
@click.option("--name", required=True, help="Experiment name (must be unique)")
@click.option("--source", required=True,
              type=click.Choice(["diann_tabular", "diann_raw", "maxquant", "spectronaut", "generic_format"]),
              help="Data source format")
@click.option("--labelling-method", default="lfq",
              type=click.Choice(["lfq", "tmt"]),
              help="Labelling method (default: lfq)")
@click.option("--species", default=None,
              type=click.Choice(["human", "mouse", "yeast", "chinese_hamster"]),
              help="Species")
@click.option("--description", default=None, help="Experiment description")
@click.option("--files-dir", type=click.Path(exists=False),
              help="Local directory containing data files to upload")
@click.option("--filenames", multiple=True, required=True,
              help="Filename(s) to upload (repeatable: --filenames f1.tsv --filenames f2.tsv)")
@click.option("--design-csv", type=click.Path(exists=False),
              help="Experiment design CSV: filename,sample_name,condition[,...]")
@click.option("--metadata-csv", type=click.Path(exists=False),
              help="Sample metadata CSV: sample_name,condition[,...]")
@click.option("--s3-bucket", default=None, help="S3 bucket (instead of local upload)")
@click.option("--s3-prefix", default=None, help="S3 key prefix")
@click.option("--dry-run", is_flag=True, help="Show payload without sending")
@click.option("--no-start", is_flag=True, help="Create + upload but don't start workflow")
@click.option("--no-upload", is_flag=True, help="Create experiment but skip file upload")
@click.option("--format", "output_format", default="json",
              type=click.Choice(["json", "ids-only"]),
              help="Output format (default: json)")
def experiments_create(name, source, labelling_method, species, description,
                       files_dir, filenames, design_csv, metadata_csv,
                       s3_bucket, s3_prefix, dry_run, no_start, no_upload,
                       output_format):
    """Create an experiment with data files.

    Full flow: create experiment → upload files to S3 → start processing workflow.

    Examples:
      md experiments create \\
        --name "My DIA-NN study" \\
        --source diann_tabular \\
        --files-dir ./data \\
        --filenames report.pg_matrix.tsv \\
        --filenames report.pr_matrix.tsv \\
        --design-csv experiment_design.csv \\
        --metadata-csv sample_metadata.csv

    Design CSV format:
      filename,sample_name,condition
      report.pg_matrix.tsv,sample1,treatment
      report.pg_matrix.tsv,sample2,control

    Metadata CSV format:
      sample_name,condition
      sample1,treatment
      sample2,control
    """
    # Parse CSVs into array-of-arrays
    experiment_design = []
    if design_csv and Path(design_csv).exists():
        experiment_design = read_csv_as_arrays(design_csv)
    elif design_csv:
        click.echo(f"Warning: design CSV not found: {design_csv}", err=True)

    sample_metadata = []
    if metadata_csv and Path(metadata_csv).exists():
        sample_metadata = read_csv_as_arrays(metadata_csv)
    elif metadata_csv:
        click.echo(f"Warning: metadata CSV not found: {metadata_csv}", err=True)

    # Auto-generate design/metadata from filenames if not provided
    if not experiment_design and filenames:
        experiment_design = [["filename", "sample_name", "condition"]]
        for i, fn in enumerate(filenames):
            experiment_design.append([fn, f"sample_{i+1}", f"condition_{(i % 2) + 1}"])
    if not sample_metadata and len(experiment_design) > 1:
        seen = set()
        sample_metadata = [["sample_name", "condition"]]
        for row in experiment_design[1:]:
            sn = row[1]
            if sn not in seen:
                sample_metadata.append([sn, row[2]])
                seen.add(sn)

    if dry_run:
        payload = {
            "experiment": {
                "name": name, "source": source,
                "labelling_method": labelling_method,
                "file_location": "s3" if s3_bucket else "local",
                "experiment_design": experiment_design,
                "sample_metadata": sample_metadata,
                "filenames": list(filenames),
            }
        }
        if species:
            payload["experiment"]["species"] = species
        click.echo("=== DRY RUN ===")
        output_json(payload)
        return

    client = get_client()
    click.echo(f"Creating experiment '{name}'...")
    result = client.create_experiment(
        name=name, source=source, filenames=list(filenames),
        experiment_design=experiment_design,
        sample_metadata=sample_metadata,
        labelling_method=labelling_method,
        species=species, description=description,
        s3_bucket=s3_bucket, s3_prefix=s3_prefix,
    )

    exp_id = result.get("id") or result.get("experiment_id")
    click.echo(f"✓ Experiment created: {exp_id}", err=True)

    # Upload files via presigned S3 URLs
    uploads = result.get("uploads") or []
    presigned_urls = {u["filename"]: u["url"] for u in uploads if "url" in u}
    if not presigned_urls:
        presigned_urls = result.get("presigned_urls") or result.get("urls") or {}

    if presigned_urls and files_dir and not no_upload:
        files_path = Path(files_dir)
        click.echo(f"Uploading {len(presigned_urls)} files to S3...", err=True)
        for filename, url in presigned_urls.items():
            file_path = files_path / filename
            if file_path.exists():
                click.echo(f"  ↑ {filename} ({file_path.stat().st_size:,} bytes)...", err=True)
                try:
                    client.upload_file(url, file_path)
                    click.echo(f"  ✓ {filename}", err=True)
                except Exception as e:
                    click.echo(f"  ✗ {filename}: {e}", err=True)
            else:
                click.echo(f"  ✗ {filename} not found at {file_path}", err=True)

        if not no_start:
            click.echo("Starting workflow...", err=True)
            try:
                client.start_workflow(exp_id)
                click.echo("✓ Workflow started", err=True)
            except Exception as e:
                click.echo(f"✗ Workflow start failed: {e}", err=True)

    output_result(result, output_format)


@experiments.command("wait")
@click.argument("experiment_id")
@click.option("--timeout", default=600, help="Max seconds to wait (default: 600)")
@click.option("--interval", default=10, help="Poll interval in seconds (default: 10)")
def experiments_wait(experiment_id, timeout, interval):
    """Wait for experiment processing to complete.

    Polls the experiment status until it reaches a terminal state
    (completed, failed, cancelled) or the timeout is reached.
    """
    client = get_client()
    terminal = {"completed", "done", "failed", "error", "cancelled", "processing_failed"}
    wait_for_status(client, client.get_experiment, experiment_id, terminal, timeout, interval)


@experiments.command("cancel")
@click.argument("experiment_id")
def experiments_cancel(experiment_id):
    """Cancel a processing experiment."""
    client = get_client()
    result = client.cancel_experiment(experiment_id)
    click.echo(f"✓ Experiment {experiment_id} cancelled")
    output_json(result)


# =============================================================================
# DESIGN (experiment design helpers)
# =============================================================================

@cli.group()
def design():
    """Experiment design helpers.

    Tools for discovering sample names and constructing experiment designs
    without needing to create CSV files manually.
    """
    pass


@design.command("infer")
@click.argument("experiment_id")
@click.option("--format", "output_format", default="json",
              type=click.Choice(["json", "csv", "ids-only"]),
              help="Output format (default: json)")
def design_infer(experiment_id, output_format):
    """Infer sample design from an uploaded experiment.

    Reads the intensity dataset to discover actual sample names, then
    suggests a design template the agent can use with --conditions.

    This solves the "what are my sample names?" problem — after uploading
    a DIA-NN/MaxQuant file, run this to see the real sample identifiers.

    Examples:
      md design infer <experiment-id>
      md design infer <experiment-id> --format csv > design.csv
    """
    client = get_client()

    # Find the intensity dataset
    datasets_result = client.list_datasets(experiment_id)
    if not isinstance(datasets_result, list):
        datasets_result = datasets_result.get("datasets", [])

    intensity_ds = None
    for ds in datasets_result:
        ds_type = (ds.get("type") or ds.get("run_type", "")).upper()
        if "INTENSITY" in ds_type or "NORMALISATION" in ds_type:
            intensity_ds = ds
            break

    if not intensity_ds:
        click.echo("Error: No intensity dataset found. Is the experiment still processing?", err=True)
        click.echo(f"  Check status: md experiments wait {experiment_id}", err=True)
        sys.exit(1)

    ds_id = intensity_ds.get("id")
    click.echo(f"Found intensity dataset: {ds_id}", err=True)

    # Get sample names from the intensity table headers or metadata
    try:
        headers = client.get_table_headers(experiment_id, ds_id, "Protein_Intensity")
        # Sample names are typically the non-metadata columns
        meta_cols = {"protein_id", "protein_name", "gene_name", "description",
                     "accession", "uniprot_id", "protein_group", "majority_protein_ids"}
        samples = [h for h in headers if h.lower() not in meta_cols and not h.startswith("_")]
    except Exception:
        # Fallback: try Protein_Metadata or experiment metadata
        try:
            exp = client.get_experiment(experiment_id)
            exp_design = exp.get("experiment_design", [])
            if exp_design and len(exp_design) > 1:
                # First row is headers, find sample_name column
                header_row = [h.lower() for h in exp_design[0]]
                sn_idx = next((i for i, h in enumerate(header_row) if "sample" in h), 0)
                samples = [row[sn_idx] for row in exp_design[1:]]
            else:
                samples = []
        except Exception:
            samples = []

    if not samples:
        click.echo("Warning: Could not extract sample names automatically.", err=True)
        click.echo("  The experiment may still be processing, or the data format is unusual.", err=True)
        sys.exit(1)

    if output_format == "csv":
        click.echo("sample_name,condition")
        for s in samples:
            click.echo(f"{s},")  # Empty condition for user to fill
    elif output_format == "ids-only":
        for s in samples:
            click.echo(s)
    else:
        result = {
            "experiment_id": experiment_id,
            "dataset_id": ds_id,
            "sample_count": len(samples),
            "samples": samples,
            "conditions_template": ",".join(f"{s}:CONDITION" for s in samples),
            "usage_hint": f"md analysis pairwise --conditions '{','.join(f'{s}:CONDITION' for s in samples[:2])},...'"
        }
        output_result(result, "json")


# =============================================================================
# DATASETS
# =============================================================================

@cli.group()
def datasets():
    """Dataset and analysis management.

    Datasets are analysis results attached to experiments. Each dataset
    is created by running a job (analysis pipeline) on input datasets.

    Types:
      INTENSITY                  - Raw/normalised intensity data
      PAIRWISE                   - Differential expression (limma)
      DOSE_RESPONSE              - Dose-response curves (drc)
      ANOVA                      - Multi-condition ANOVA
      ENRICHMENT                 - Pathway enrichment
      NORMALISATION_AND_IMPUTATION - Normalised + imputed intensities

    States: PROCESSING → COMPLETED | FAILED | CANCELLED
    """
    pass


@datasets.command("list")
@click.argument("experiment_id")
@click.option("--type", "filter_type", default=None,
              help="Filter by dataset type (e.g., INTENSITY, PAIRWISE, DOSE_RESPONSE, ANOVA)")
@click.option("--format", "output_format", default="json",
              type=click.Choice(["json", "ids-only", "table"]),
              help="Output format (default: json)")
def datasets_list(experiment_id, filter_type, output_format):
    """List all datasets for an experiment.

    Shows dataset ID, name, type, and processing state.

    Examples:
      md datasets list <exp-id>
      md datasets list <exp-id> --type INTENSITY --format ids-only
    """
    client = get_client()
    result = client.list_datasets(experiment_id)
    if isinstance(result, list) and filter_type:
        filter_upper = filter_type.upper()
        result = [ds for ds in result if filter_upper in
                  (ds.get("type") or ds.get("run_type", "")).upper()]

    if output_format == "ids-only":
        items = result if isinstance(result, list) else []
        for ds in items:
            click.echo(ds.get("id", ""))
    elif output_format == "table" and isinstance(result, list):
        click.echo(f"Found {len(result)} datasets:")
        for ds in result:
            state = ds.get("state") or ds.get("status", "?")
            name = ds.get("name", "?")
            dtype = ds.get("type", "?")
            did = ds.get("id", "?")
            click.echo(f"  [{state}] {dtype}: {name} ({did})")
    else:
        output_result(result, output_format)


@datasets.command("get")
@click.argument("dataset_id")
@click.option("--experiment-id", "-e", default=None,
              help="Experiment UUID (required if direct dataset fetch fails)")
def datasets_get(dataset_id, experiment_id):
    """Get dataset details including tables and parameters.

    Some deployments don't support GET /datasets/:id directly.
    In that case, provide --experiment-id so the CLI can look up the
    dataset from the experiment's dataset list.

    Examples:
      md datasets get <dataset-id>
      md datasets get <dataset-id> -e <experiment-id>
    """
    client = get_client()
    try:
        result = client.get_dataset(dataset_id, experiment_id=experiment_id)
        output_json(result)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if not experiment_id:
            click.echo("Tip: Try again with --experiment-id <UUID>", err=True)
        sys.exit(1)


@datasets.command("create")
@click.option("--input-dataset-ids", required=True, multiple=True,
              help="Input dataset UUID(s) (repeatable)")
@click.option("--name", required=True, help="Name for the result dataset")
@click.option("--job-slug", required=True,
              type=click.Choice(["pairwise_comparison", "dose_response", "anova",
                                 "enrichment", "intensity", "flexicomp"]),
              help="Analysis type to run")
@click.option("--params-json", type=click.Path(exists=True),
              help="JSON file with job_run_params")
@click.option("--sample-names", multiple=True, help="Sample names (repeatable)")
def datasets_create(input_dataset_ids, name, job_slug, params_json, sample_names):
    """Create a dataset by running an analysis job.

    The --params-json file should contain the job-specific parameters.
    Use 'md jobs list' to see available job types and their parameters.

    Example:
      md datasets create \\
        --input-dataset-ids abc123 \\
        --name "Treatment vs Control" \\
        --job-slug pairwise_comparison \\
        --params-json pairwise_params.json
    """
    client = get_client()
    params = {}
    if params_json:
        with open(params_json) as f:
            params = json.load(f)

    result = client.create_dataset(
        input_dataset_ids=list(input_dataset_ids),
        name=name,
        job_slug=job_slug,
        job_run_params=params,
        sample_names=list(sample_names) if sample_names else None,
    )
    click.echo(f"✓ Dataset creation started")
    output_json(result)


@datasets.command("retry")
@click.argument("dataset_id")
def datasets_retry(dataset_id):
    """Retry a failed dataset."""
    client = get_client()
    result = client.retry_dataset(dataset_id)
    click.echo(f"✓ Dataset {dataset_id} retry initiated")
    output_json(result)


@datasets.command("delete")
@click.argument("dataset_id")
@click.option("--yes", is_flag=True, help="Skip confirmation")
def datasets_delete(dataset_id, yes):
    """Delete a dataset."""
    if not yes:
        click.confirm(f"Delete dataset {dataset_id}?", abort=True)
    client = get_client()
    client.delete_dataset(dataset_id)
    click.echo(f"✓ Dataset {dataset_id} deleted")


@datasets.command("wait")
@click.argument("dataset_id")
@click.option("--timeout", default=600, help="Max seconds to wait (default: 600)")
@click.option("--interval", default=10, help="Poll interval in seconds (default: 10)")
def datasets_wait(dataset_id, timeout, interval):
    """Wait for dataset processing to complete.

    Polls until COMPLETED, FAILED, or CANCELLED.
    """
    client = get_client()
    terminal = {"completed", "done", "failed", "error", "cancelled"}
    wait_for_status(client, client.get_dataset, dataset_id, terminal, timeout, interval)


# =============================================================================
# ANALYSIS (high-level shortcuts)
# =============================================================================

@cli.group()
def analysis():
    """Run analyses with guided parameter input.

    These commands wrap 'md datasets create' with structured parameters
    for each analysis type, so you don't need to write raw JSON.
    """
    pass


@analysis.command("pairwise")
@click.option("--input-dataset-id", required=True, help="Intensity dataset UUID")
@click.option("--name", required=True, help="Result name")
@click.option("--design-csv", default=None, type=click.Path(exists=True),
              help="CSV with sample_name and condition columns")
@click.option("--conditions", default=None,
              help="Inline conditions: 'sample1:Control,sample2:Treatment' (alternative to --design-csv)")
@click.option("--condition-column", required=True, help="Column name for conditions")
@click.option("--comparisons", required=True,
              help="Comparison pairs: Treatment:Control,Drug:Vehicle")
@click.option("--normalise", default="median",
              type=click.Choice(["median", "quantile", "none"]),
              help="Normalisation method (default: median)")
@click.option("--log-intensities/--no-log-intensities", default=True,
              help="Apply log2 transform (default: yes)")
@click.option("--use-imputed/--no-imputed", default=True,
              help="Use imputed intensities (default: yes)")
@click.option("--filter-method", default="percentage",
              type=click.Choice(["percentage", "count"]))
@click.option("--filter-threshold", default=0.66, type=float,
              help="Filter threshold (0.66 = 66%% for percentage)")
@click.option("--filter-logic", default="at least one condition",
              type=click.Choice(["at least one condition", "all conditions", "full experiment"]))
@click.option("--sample-names", multiple=True, help="Subset of samples to use")
@click.option("--format", "output_format", default="json",
              type=click.Choice(["json", "ids-only"]),
              help="Output format (default: json)")
def analysis_pairwise(input_dataset_id, name, design_csv, conditions, condition_column,
                      comparisons, normalise, log_intensities, use_imputed,
                      filter_method, filter_threshold, filter_logic, sample_names,
                      output_format):
    """Run pairwise comparison (differential expression via limma).

    Compares two conditions to find differentially expressed proteins.
    Uses the limma R package for robust statistical testing.

    Provide sample-to-condition mapping via EITHER --design-csv OR --conditions:

    Example (with CSV):
      md analysis pairwise \\
        --input-dataset-id abc123 \\
        --name "Treatment vs Control" \\
        --design-csv design.csv \\
        --condition-column condition \\
        --comparisons "Treatment:Control"

    Example (inline, no CSV needed):
      md analysis pairwise \\
        --input-dataset-id abc123 \\
        --name "Treatment vs Control" \\
        --conditions "s1:Control,s2:Control,s3:Control,s4:Treatment,s5:Treatment,s6:Treatment" \\
        --condition-column condition \\
        --comparisons "Treatment:Control"

    Tip: Run 'md design infer <EXP_ID>' to discover actual sample names.
    """
    client = get_client()
    design = resolve_design(design_csv, conditions, condition_column)
    comp_pairs = parse_comparisons(comparisons)

    click.echo(f"Creating pairwise comparison '{name}'...")
    result = client.create_pairwise_comparison(
        input_dataset_ids=[input_dataset_id],
        name=name,
        experiment_design=design,
        condition_column=condition_column,
        condition_comparisons=comp_pairs,
        log_intensities=log_intensities,
        use_imputed_intensities=use_imputed,
        normalise=normalise,
        filter_method=filter_method,
        filter_threshold=filter_threshold,
        filter_logic=filter_logic,
        sample_names=list(sample_names) if sample_names else None,
    )
    click.echo("✓ Pairwise comparison submitted", err=True)
    output_result(result, output_format)


@analysis.command("dose-response")
@click.option("--input-dataset-id", required=True, help="Intensity dataset UUID")
@click.option("--name", required=True, help="Result name")
@click.option("--design-csv", default=None, type=click.Path(exists=True),
              help="CSV with sample_name and dose columns")
@click.option("--conditions", default=None,
              help="Inline dose mapping: 'sample1:0,sample2:10,sample3:100' (alternative to --design-csv)")
@click.option("--control-samples", required=True, multiple=True,
              help="Control sample name(s) (repeatable)")
@click.option("--normalise", default="sum",
              type=click.Choice(["sum", "median", "none"]),
              help="Normalisation method (default: sum)")
@click.option("--log-intensities/--no-log-intensities", default=True)
@click.option("--use-imputed/--no-imputed", default=True)
@click.option("--span-rollmean-k", default=1, type=float,
              help="Rolling mean span (1 to N distinct doses)")
@click.option("--prop-required", default=0.5, type=float,
              help="Proportion of samples required per protein (0-1)")
@click.option("--sample-names", multiple=True)
@click.option("--format", "output_format", default="json",
              type=click.Choice(["json", "ids-only"]),
              help="Output format (default: json)")
def analysis_dose_response(input_dataset_id, name, design_csv, conditions,
                           control_samples, normalise, log_intensities, use_imputed,
                           span_rollmean_k, prop_required, sample_names, output_format):
    """Run dose-response analysis (curve fitting via R drc package).

    Fits dose-response curves to protein abundance across dose levels.

    Provide sample-to-dose mapping via EITHER --design-csv OR --conditions.

    Example (with CSV):
      md analysis dose-response \\
        --input-dataset-id abc123 \\
        --name "Dose Response" \\
        --design-csv dose_design.csv \\
        --control-samples DMSO_1 --control-samples DMSO_2

    Example (inline):
      md analysis dose-response \\
        --input-dataset-id abc123 \\
        --name "Dose Response" \\
        --conditions "s1:0,s2:0,s3:10,s4:100,s5:1000" \\
        --control-samples s1 --control-samples s2
    """
    client = get_client()
    design = resolve_design(design_csv, conditions, "dose")

    click.echo(f"Creating dose-response analysis '{name}'...")
    result = client.create_dose_response(
        input_dataset_ids=[input_dataset_id],
        name=name,
        experiment_design=design,
        control_samples=list(control_samples),
        log_intensities=log_intensities,
        use_imputed_intensities=use_imputed,
        normalise=normalise,
        span_rollmean_k=span_rollmean_k,
        prop_required_in_protein=prop_required,
        sample_names=list(sample_names) if sample_names else None,
    )
    click.echo("✓ Dose-response analysis submitted", err=True)
    output_result(result, output_format)


@analysis.command("anova")
@click.option("--input-dataset-id", required=True, help="Intensity dataset UUID")
@click.option("--name", required=True, help="Result name")
@click.option("--design-csv", default=None, type=click.Path(exists=True),
              help="CSV with sample_name and condition columns")
@click.option("--conditions", default=None,
              help="Inline conditions: 'sample1:GroupA,sample2:GroupB,...' (alternative to --design-csv)")
@click.option("--condition-column", required=True, help="Column for grouping")
@click.option("--normalise", default="median",
              type=click.Choice(["median", "quantile", "none"]))
@click.option("--log-intensities/--no-log-intensities", default=True)
@click.option("--use-imputed/--no-imputed", default=True)
@click.option("--filter-method", default="percentage",
              type=click.Choice(["percentage", "count"]))
@click.option("--filter-threshold", default=0.66, type=float)
@click.option("--filter-logic", default="at least one condition",
              type=click.Choice(["at least one condition", "all conditions", "full experiment"]))
@click.option("--sample-names", multiple=True)
@click.option("--format", "output_format", default="json",
              type=click.Choice(["json", "ids-only"]),
              help="Output format (default: json)")
def analysis_anova(input_dataset_id, name, design_csv, conditions, condition_column,
                   normalise, log_intensities, use_imputed,
                   filter_method, filter_threshold, filter_logic, sample_names,
                   output_format):
    """Run ANOVA (multi-condition analysis).

    Tests for differential expression across 3+ conditions simultaneously.

    Example (with CSV):
      md analysis anova \\
        --input-dataset-id abc123 \\
        --name "3-way ANOVA" \\
        --design-csv design.csv \\
        --condition-column treatment

    Example (inline):
      md analysis anova \\
        --input-dataset-id abc123 \\
        --name "3-way ANOVA" \\
        --conditions "s1:A,s2:A,s3:B,s4:B,s5:C,s6:C" \\
        --condition-column treatment
    """
    client = get_client()
    design = resolve_design(design_csv, conditions, condition_column)

    click.echo(f"Creating ANOVA analysis '{name}'...")
    result = client.create_anova(
        input_dataset_ids=[input_dataset_id],
        name=name,
        experiment_design=design,
        condition_column=condition_column,
        log_intensities=log_intensities,
        use_imputed_intensities=use_imputed,
        normalise=normalise,
        filter_method=filter_method,
        filter_threshold=filter_threshold,
        filter_logic=filter_logic,
        sample_names=list(sample_names) if sample_names else None,
    )
    click.echo("✓ ANOVA analysis submitted", err=True)
    output_result(result, output_format)


# =============================================================================
# TABLES
# =============================================================================

@cli.group()
def tables():
    """Access dataset table data.

    Each dataset contains one or more tables (stored as Parquet in S3):

    INTENSITY datasets:
      Protein_Intensity, Protein_Metadata, Peptide_Intensity, Peptide_Metadata

    PAIRWISE datasets:
      output_comparisons, runtime_metadata

    DOSE_RESPONSE datasets:
      output_curves, output_volcanoes, input_drc, runtime_metadata

    ANOVA datasets:
      anova_results, runtime_metadata
    """
    pass


@tables.command("list")
@click.argument("experiment_id")
@click.argument("dataset_id")
def tables_list(experiment_id, dataset_id):
    """List available table names for a dataset.

    Shows what tables exist so you don't have to guess names.

    Known table names by dataset type:
      INTENSITY:     Protein_Intensity, Protein_Metadata, Peptide_Intensity, Peptide_Metadata
      PAIRWISE:      output_comparisons, runtime_metadata
      DOSE_RESPONSE: output_curves, output_volcanoes, input_drc, runtime_metadata
      ANOVA:         anova_results, runtime_metadata

    Example:
      md tables list <exp-id> <dataset-id>
    """
    client = get_client()

    # Try to get the dataset to determine its type
    try:
        ds = client.get_dataset(dataset_id, experiment_id=experiment_id)
        ds_type = (ds.get("type") or ds.get("run_type", "UNKNOWN")).upper()
    except Exception:
        ds_type = "UNKNOWN"

    # Known table names by type
    known_tables = {
        "INTENSITY": ["Protein_Intensity", "Protein_Metadata", "Peptide_Intensity", "Peptide_Metadata"],
        "NORMALISATION_AND_IMPUTATION": ["Protein_Intensity", "Protein_Metadata", "Peptide_Intensity", "Peptide_Metadata"],
        "PAIRWISE": ["output_comparisons", "runtime_metadata"],
        "PAIRWISE_COMPARISON": ["output_comparisons", "runtime_metadata"],
        "DOSE_RESPONSE": ["output_curves", "output_volcanoes", "input_drc", "runtime_metadata"],
        "ANOVA": ["anova_results", "runtime_metadata"],
    }

    tables_for_type = known_tables.get(ds_type, [])

    # Try to validate which tables actually exist by attempting headers
    verified = []
    for tname in tables_for_type:
        try:
            client.get_table_headers(experiment_id, dataset_id, tname)
            verified.append(tname)
        except Exception:
            pass

    if verified:
        click.echo(f"Tables for dataset {dataset_id} (type: {ds_type}):")
        for t in verified:
            click.echo(f"  {t}")
    elif tables_for_type:
        click.echo(f"Expected tables for {ds_type} dataset (not yet verified):")
        for t in tables_for_type:
            click.echo(f"  {t}")
    else:
        click.echo(f"Dataset type: {ds_type}")
        click.echo("Table names unknown for this type. Try 'md tables headers <exp> <ds> <table_name>' to probe.")


@tables.command("headers")
@click.argument("experiment_id")
@click.argument("dataset_id")
@click.argument("table_name")
def tables_headers(experiment_id, dataset_id, table_name):
    """Get column headers for a table.

    Example:
      md tables headers <exp-id> <dataset-id> Protein_Intensity
    """
    client = get_client()
    result = client.get_table_headers(experiment_id, dataset_id, table_name)
    if isinstance(result, list):
        click.echo(f"Columns ({len(result)}):")
        for col in result:
            click.echo(f"  {col}")
    else:
        output_json(result)


@tables.command("download")
@click.argument("experiment_id")
@click.argument("dataset_id")
@click.argument("table_name")
@click.option("--output", "-o", type=click.Path(), help="Output file path")
@click.option("--format", "fmt", default="csv",
              type=click.Choice(["csv", "parquet"]),
              help="Download format (default: csv)")
def tables_download(experiment_id, dataset_id, table_name, output, fmt):
    """Download a dataset table as CSV or Parquet.

    Example:
      md tables download <exp-id> <ds-id> Protein_Intensity -o results.csv
      md tables download <exp-id> <ds-id> output_comparisons --format parquet -o results.parquet
    """
    client = get_client()
    response = client.get_table(experiment_id, dataset_id, table_name, format=fmt)

    if output:
        out_path = Path(output)
        with open(out_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        click.echo(f"✓ Saved to {out_path} ({out_path.stat().st_size:,} bytes)")
    else:
        # Stream to stdout
        for chunk in response.iter_content(chunk_size=8192):
            sys.stdout.buffer.write(chunk)


@tables.command("query")
@click.argument("experiment_id")
@click.argument("dataset_id")
@click.argument("table_name")
@click.option("--sql", required=True, help="SQL query to execute")
def tables_query(experiment_id, dataset_id, table_name, sql):
    """Run a SQL query on a dataset table.

    Supports standard SQL syntax against the table data.

    Examples:
      md tables query <exp> <ds> output_comparisons \\
        --sql "SELECT protein_id, log2fc, pvalue FROM data WHERE pvalue < 0.05"

      md tables query <exp> <ds> Protein_Intensity \\
        --sql "SELECT * FROM data LIMIT 10"
    """
    client = get_client()
    result = client.query_table(experiment_id, dataset_id, table_name, sql)
    output_json(result)


# =============================================================================
# JOBS (available analysis types)
# =============================================================================

@cli.group()
def jobs():
    """Browse available analysis job types.

    Jobs are the analysis pipelines that create datasets.
    Use 'md jobs list' to see what analyses are available.
    """
    pass


@jobs.command("list")
def jobs_list():
    """List available analysis job types and their parameters."""
    client = get_client()
    try:
        result = client.list_dataset_jobs()
        if isinstance(result, list):
            click.echo(f"Available analysis types ({len(result)}):")
            for job in result:
                slug = job.get("slug", "?")
                name = job.get("name", "?")
                desc = job.get("description", "")
                click.echo(f"  {slug}: {name}")
                if desc:
                    click.echo(f"    {desc}")
        else:
            output_json(result)
    except Exception:
        # Fallback: show known job types from repo analysis
        click.echo("Available analysis types (from platform documentation):")
        click.echo("  pairwise_comparison : Two-condition differential expression (limma)")
        click.echo("  dose_response       : Dose-response curve fitting (drc)")
        click.echo("  anova               : Multi-condition ANOVA")
        click.echo("  enrichment          : Pathway enrichment analysis")
        click.echo("  intensity           : Intensity normalisation")
        click.echo("  flexicomp           : Flexible comparison")


# =============================================================================
# WORKSPACES
# =============================================================================

@cli.group()
def workspaces():
    """Workspace management.

    Workspaces group multiple experiments for comparative analysis.
    They contain tabs with visualisation modules arranged in a 12-column grid.
    """
    pass


@workspaces.command("list")
def workspaces_list():
    """List all workspaces."""
    client = get_client()
    try:
        result = client.list_workspaces()
        if isinstance(result, list):
            click.echo(f"Found {len(result)} workspaces:")
            for ws in result:
                name = ws.get("name", "?")
                wid = ws.get("id", "?")
                click.echo(f"  {name} ({wid})")
        else:
            output_json(result)
    except Exception as e:
        click.echo(f"Error listing workspaces: {e}", err=True)
        click.echo("Note: Workspace listing may require session cookie auth.", err=True)
        sys.exit(1)


@workspaces.command("get")
@click.argument("workspace_id")
def workspaces_get(workspace_id):
    """Get workspace details."""
    client = get_client()
    output_json(client.get_workspace(workspace_id))


@workspaces.command("create")
@click.option("--name", required=True, help="Workspace name")
@click.option("--description", default=None, help="Description")
def workspaces_create(name, description):
    """Create a new workspace."""
    client = get_client()
    result = client.create_workspace(name, description)
    click.echo(f"✓ Workspace created")
    output_json(result)


@workspaces.command("delete")
@click.argument("workspace_id")
@click.option("--yes", is_flag=True, help="Skip confirmation")
def workspaces_delete(workspace_id, yes):
    """Delete a workspace."""
    if not yes:
        click.confirm(f"Delete workspace {workspace_id}?", abort=True)
    client = get_client()
    client.delete_workspace(workspace_id)
    click.echo(f"✓ Workspace {workspace_id} deleted")


@workspaces.command("add-experiment")
@click.argument("workspace_id")
@click.argument("experiment_id")
def workspaces_add_experiment(workspace_id, experiment_id):
    """Add an experiment to a workspace."""
    client = get_client()
    result = client.add_experiment_to_workspace(workspace_id, experiment_id)
    click.echo(f"✓ Experiment added to workspace")
    output_json(result)


@workspaces.command("tabs")
@click.argument("workspace_id")
def workspaces_tabs(workspace_id):
    """List tabs in a workspace."""
    client = get_client()
    result = client.list_workspace_tabs(workspace_id)
    if isinstance(result, list):
        click.echo(f"Tabs ({len(result)}):")
        for tab in result:
            name = tab.get("name", "?")
            tid = tab.get("id", "?")
            modules = len(tab.get("layout", {}).get("modules", []))
            click.echo(f"  {name} ({tid}) - {modules} modules")
    else:
        output_json(result)


@workspaces.command("datasets")
@click.argument("workspace_id")
def workspaces_datasets(workspace_id):
    """List datasets in a workspace."""
    client = get_client()
    result = client.list_workspace_datasets(workspace_id)
    if isinstance(result, list):
        click.echo(f"Datasets ({len(result)}):")
        for ds in result:
            name = ds.get("name", "?")
            dtype = ds.get("type", "?")
            state = ds.get("state", "?")
            click.echo(f"  [{state}] {dtype}: {name} ({ds.get('id', '?')})")
    else:
        output_json(result)


# =============================================================================
# VISUALISATIONS
# =============================================================================

@cli.group()
def viz():
    """Generate scientific visualisations.

    All viz commands return Plotly JSON specifications.
    Save output to a file and open in a browser, or pipe to jq for inspection.

    Output formats: json (default), html, png, svg (where supported)
    """
    pass


@viz.command("volcano")
@click.option("--workspace-id", required=True, help="Workspace UUID")
@click.option("--dataset-id", required=True, help="Pairwise comparison dataset UUID")
@click.option("--comparison", required=True, help="Comparison name (e.g. Treatment_vs_Control)")
@click.option("--fc-threshold", default=1.0, type=float, help="Fold-change threshold (default: 1.0)")
@click.option("--pvalue-threshold", default=0.05, type=float, help="P-value threshold (default: 0.05)")
@click.option("--output", "-o", type=click.Path(), help="Save JSON to file")
def viz_volcano(workspace_id, dataset_id, comparison, fc_threshold, pvalue_threshold, output):
    """Generate a volcano plot from pairwise comparison results.

    Shows log2 fold-change vs -log10 p-value for differential expression.

    Example:
      md viz volcano \\
        --workspace-id ws123 \\
        --dataset-id ds456 \\
        --comparison "Treatment_vs_Control" \\
        -o volcano.json
    """
    client = get_client()
    result = client.volcano_plot(workspace_id, dataset_id, comparison,
                                  fc_threshold=fc_threshold, pvalue_threshold=pvalue_threshold)
    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


@viz.command("heatmap")
@click.option("--workspace-id", required=True, help="Workspace UUID")
@click.option("--dataset-ids", required=True, multiple=True, help="Dataset UUID(s)")
@click.option("--cluster-dist", default=0.5, type=float, help="Clustering distance (0-1)")
@click.option("--z-score/--no-z-score", default=True, help="Apply z-score normalisation")
@click.option("--output", "-o", type=click.Path())
def viz_heatmap(workspace_id, dataset_ids, cluster_dist, z_score, output):
    """Generate an intensity heatmap with hierarchical clustering.

    Example:
      md viz heatmap --workspace-id ws123 --dataset-ids ds456
    """
    client = get_client()
    result = client.heatmap(workspace_id, list(dataset_ids),
                            cluster_dist=cluster_dist, z_score=z_score)
    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


@viz.command("pca")
@click.option("--workspace-id", required=True, help="Workspace UUID")
@click.option("--dataset-ids", required=True, multiple=True, help="Intensity dataset UUID(s)")
@click.option("--method", default="pca",
              type=click.Choice(["pca", "tsne", "umap"]),
              help="Dimensionality reduction method")
@click.option("--colour-by", default="condition", help="Metadata column for colouring")
@click.option("--shape-by", default=None, help="Metadata column for point shapes")
@click.option("--scaling", default="zscore",
              type=click.Choice(["zscore", "pareto", "none"]))
@click.option("--output", "-o", type=click.Path())
def viz_pca(workspace_id, dataset_ids, method, colour_by, shape_by, scaling, output):
    """Generate PCA, t-SNE, or UMAP plot.

    Reduces high-dimensional protein data to 2D for sample clustering.

    Example:
      md viz pca --workspace-id ws123 --dataset-ids ds456 --method umap
    """
    client = get_client()
    result = client.dimensionality_reduction(
        workspace_id, list(dataset_ids),
        method=method, colour_by=colour_by, shape_by=shape_by,
        scaling_method=scaling,
    )
    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


@viz.command("box-plot")
@click.option("--workspace-id", required=True, help="Workspace UUID")
@click.option("--dataset-ids", required=True, multiple=True)
@click.option("--proteins", required=True, multiple=True, help="Protein name(s)")
@click.option("--colour-by", default="condition")
@click.option("--output", "-o", type=click.Path())
def viz_box_plot(workspace_id, dataset_ids, proteins, colour_by, output):
    """Generate box plots for specific proteins across conditions.

    Example:
      md viz box-plot --workspace-id ws123 --dataset-ids ds456 --proteins TP53 --proteins BRCA1
    """
    client = get_client()
    result = client.box_plot(workspace_id, list(dataset_ids), list(proteins), colour_by)
    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


@viz.command("dose-response")
@click.option("--experiment-id", required=True)
@click.option("--dataset-ids", required=True, multiple=True)
@click.option("--proteins", multiple=True, help="Specific proteins to plot")
@click.option("--output", "-o", type=click.Path())
def viz_dose_response(experiment_id, dataset_ids, proteins, output):
    """Generate dose-response curve plots.

    Example:
      md viz dose-response --experiment-id exp123 --dataset-ids ds456 --proteins TP53
    """
    client = get_client()
    result = client.dose_response_plot(
        experiment_id, list(dataset_ids),
        proteins=list(proteins) if proteins else None,
    )
    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


@viz.command("anova-volcano")
@click.option("--workspace-id", required=True)
@click.option("--dataset-id", required=True, help="ANOVA dataset UUID")
@click.option("--output", "-o", type=click.Path())
def viz_anova_volcano(workspace_id, dataset_id, output):
    """Generate ANOVA volcano plot (multi-condition)."""
    client = get_client()
    result = client.anova_volcano_plot(workspace_id, dataset_id)
    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


@viz.command("qc")
@click.option("--workspace-id", required=True)
@click.option("--dataset-ids", required=True, multiple=True)
@click.option("--type", "plot_type", required=True,
              type=click.Choice(["intensity-distribution", "missing-values-feature",
                                 "missing-values-sample", "cv-distribution"]),
              help="QC plot type")
@click.option("--output", "-o", type=click.Path())
def viz_qc(workspace_id, dataset_ids, plot_type, output):
    """Generate quality control plots.

    Types:
      intensity-distribution  - Distribution of log2 intensities per sample
      missing-values-feature  - Missing values by protein/peptide
      missing-values-sample   - Missing values by sample
      cv-distribution         - Coefficient of variation across conditions
    """
    client = get_client()
    ds_list = list(dataset_ids)
    if plot_type == "intensity-distribution":
        result = client.intensity_distribution(workspace_id, ds_list)
    elif plot_type.startswith("missing-values"):
        by = "feature" if "feature" in plot_type else "sample"
        result = client.missing_values_plot(workspace_id, ds_list, by=by)
    elif plot_type == "cv-distribution":
        result = client.cv_distribution(workspace_id, ds_list)

    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


# =============================================================================
# ENRICHMENT
# =============================================================================

@cli.group()
def enrichment():
    """Pathway enrichment analysis.

    Run over-representation analysis (ORA) against Reactome pathways
    or explore protein-protein interactions via STRING.
    """
    pass


@enrichment.command("reactome")
@click.option("--experiment-id", required=True)
@click.option("--protein-list-id", default=None, help="Protein list UUID")
@click.option("--proteins", multiple=True, help="Protein identifiers (alternative to list)")
@click.option("--species", default="Homo sapiens",
              help="Species name (default: Homo sapiens)")
@click.option("--include-disease/--no-disease", default=True)
@click.option("--output", "-o", type=click.Path())
def enrichment_reactome(experiment_id, protein_list_id, proteins, species,
                        include_disease, output):
    """Run Reactome over-representation analysis.

    Finds enriched biological pathways from a set of proteins.

    Example:
      md enrichment reactome \\
        --experiment-id exp123 \\
        --protein-list-id pl456 \\
        --species "Homo sapiens"
    """
    client = get_client()
    result = client.reactome_ora(
        experiment_id,
        protein_list_id=protein_list_id,
        proteins=list(proteins) if proteins else None,
        species=species,
        include_disease=include_disease,
    )
    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


@enrichment.command("string")
@click.option("--experiment-id", required=True)
@click.option("--protein-list-id", default=None, help="Protein list UUID")
@click.option("--species", default=9606, type=int,
              help="NCBI Tax ID (9606=human, 10090=mouse)")
@click.option("--network-type", default="physical",
              type=click.Choice(["physical", "functional"]))
@click.option("--score", default=400, type=int,
              help="Confidence score 0-1000 (default: 400)")
@click.option("--add-nodes", default=10, type=int,
              help="Additional interactor nodes (default: 10)")
@click.option("--output", "-o", type=click.Path())
def enrichment_string(experiment_id, protein_list_id, species, network_type,
                      score, add_nodes, output):
    """Query STRING protein-protein interaction network.

    Retrieves known and predicted interactions between proteins.

    Example:
      md enrichment string \\
        --experiment-id exp123 \\
        --protein-list-id pl456 \\
        --network-type physical \\
        --score 700
    """
    client = get_client()
    result = client.string_network(
        experiment_id,
        protein_list_id=protein_list_id,
        species=species,
        network_type=network_type,
        required_score=score,
        add_nodes=add_nodes,
    )
    if output:
        with open(output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        click.echo(f"✓ Saved to {output}")
    else:
        output_json(result)


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    cli()


if __name__ == "__main__":
    main()
