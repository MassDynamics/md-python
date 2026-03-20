"""
Batch command dispatcher.

Routes batch command strings to the appropriate md-python SDK calls.
Shares a single authenticated client instance across all commands.
"""

import shlex


def run_batch(client, commands, stop_on_error=False):
    """Execute multiple commands using a shared client."""
    results = []

    for cmd_str in commands:
        entry = {"command": cmd_str, "status": "ok", "result": None, "error": None}
        try:
            args = shlex.split(cmd_str)
            if not args:
                entry["status"] = "error"
                entry["error"] = "Empty command"
            else:
                entry["result"] = _dispatch(client, args)
        except Exception as e:
            entry["status"] = "error"
            entry["error"] = str(e)
            if stop_on_error:
                results.append(entry)
                break

        results.append(entry)

    return results


def _dispatch(client, args):
    """Route a command to the appropriate SDK call."""
    cmd = args[0].lower() if args else ""

    if cmd == "health":
        return client.health.check()

    elif cmd == "auth" and len(args) > 1 and args[1] == "status":
        return client.health.check()

    elif cmd in ("uploads", "experiments", "experiment") and len(args) > 1:
        sub = args[1]
        if sub == "get" and len(args) > 2:
            if "--by-name" in args:
                # Rejoin all tokens between "get" and "--by-name" as the name
                flag_idx = args.index("--by-name")
                identifier = " ".join(args[2:flag_idx])
                upload = client.uploads.get_by_name(identifier)
            else:
                identifier = args[2]
                upload = client.uploads.get_by_id(identifier)
            return _to_dict(upload)

    elif cmd == "datasets" and len(args) > 1:
        sub = args[1]
        if sub == "list" and len(args) > 2:
            datasets = client.datasets.list_by_upload(args[2])
            return [_to_dict(d) for d in datasets] if datasets else []
        elif sub == "find-initial" and len(args) > 2:
            ds = client.datasets.find_initial_dataset(args[2])
            return _to_dict(ds) if ds else None
        elif sub == "get" and len(args) > 2:
            ds_id = args[2]
            # Need upload_id to look up — check for -e flag
            upload_id = None
            for i, a in enumerate(args):
                if a in ("-e", "--upload-id", "--experiment-id") and i + 1 < len(args):
                    upload_id = args[i + 1]
            if upload_id:
                datasets = client.datasets.list_by_upload(upload_id)
                ds = next((d for d in datasets if str(d.id) == ds_id), None)
                if ds:
                    return _to_dict(ds)
                raise Exception(f"Dataset {ds_id} not found in upload {upload_id}")
            raise Exception("Provide --upload-id or -e for dataset lookup")

    elif cmd == "jobs":
        return client.jobs.list()

    raise Exception(f"Unknown batch command: {' '.join(args)}")


def _to_dict(obj):
    """Convert SDK model to dict."""
    if obj is None:
        return None
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        return {k: str(v) if not isinstance(v, (str, int, float, bool, list, dict, type(None))) else v
                for k, v in obj.__dict__.items() if not k.startswith("_")}
    return str(obj)
