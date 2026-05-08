# API tour notebook

End-to-end walk-through of the `md-python` client. Designed to be the asset
behind a screen-recording — every section runs in a few seconds, and section 6
is dry-run by default so you don't wait for real pipelines on camera.

## Run it

```bash
.venv/bin/jupyter notebook api_tour.ipynb
# or:
.venv/bin/jupyter lab api_tour.ipynb
```

Kernel: **md-api .venv (3.11)** (baked into the notebook metadata). If you
don't have it registered yet:

```bash
.venv/bin/python -m ipykernel install --user \
    --name md-api-venv \
    --display-name 'md-api .venv (3.11)'
```

## Render to HTML / PDF

```bash
VENV=$(pwd)/../../.venv
$VENV/bin/jupyter nbconvert --to notebook --execute \
    --ExecutePreprocessor.kernel_name=md-api-venv \
    --output api_tour.executed.ipynb api_tour.ipynb

$VENV/bin/jupyter nbconvert --to html \
    --output api_tour.html api_tour.executed.ipynb

# webpdf uses headless Chromium via playwright — no LaTeX needed.
$VENV/bin/jupyter nbconvert --to webpdf \
    --output api_tour api_tour.executed.ipynb
```

`webpdf` requires a one-time install:

```bash
.venv/bin/python -m pip install 'nbconvert[webpdf]'
.venv/bin/python -m playwright install chromium
```

## Section 6 — full pipeline flow

`DRY_RUN = True` at the top of section 6 prints the call shape for each step
without firing the API. Flip it to `False` when you have your own upload to
feed it. Real pipelines take 10–40 minutes; the dry-run mode keeps the demo
tight.
