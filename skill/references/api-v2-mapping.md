# v1 to v2 API Mapping

The v2 API renames experiments to uploads and uses flat payloads.

| v1 | v2 | Notes |
|---|---|---|
| `GET /experiments/:id` | `GET /uploads/:id` | Same data, different path |
| `GET /experiments?name=` | `GET /uploads?name=` | |
| `POST /experiments` (wrapped) | `POST /uploads` (flat payload) | v2 removes `{"experiment": {...}}` wrapper |
| `POST /experiments/:id/start_workflow` | `POST /uploads/:id/start_workflow` | |
| `PUT /experiments/:id/sample_metadata` | `PUT /uploads/:id/sample_metadata` | |
| `GET /datasets?experiment_id=` | `GET /datasets?experiment_id=` | Same route, both versions |
| `POST /datasets` (wrapped) | `POST /datasets` (flat payload) | v2 removes `{"dataset": {...}}` wrapper |
| Accept: `application/vnd.md-v1+json` | Accept: `application/vnd.md-v2+json` | |

The CLI handles this automatically. Batch commands accept both `uploads get`
and `experiments get` (backward compatible).
