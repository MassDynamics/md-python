# Walkthroughs

Four notebooks that each take one thing you would normally do by clicking in the app and do it from
the API instead. They are meant to be read as much as run: every non-obvious wire format is
explained where it appears, along with what happens if you get it wrong.

Read them in this order if you are new to the client. Each one assumes the one before it.

| notebook | what it covers |
|---|---|
| `md_dr_pairwise_walkthrough.ipynb` | Upload data, then run a pairwise comparison and a per-compound dose response from a single upload |
| `md_workspace_walkthrough.ipynb` | Workspaces, tabs, the module registry and the 12-column grid |
| `md_qc_report_automation.ipynb` | The in-app Quality Control Report automation, rebuilt from the API |
| `md_pairwise_ora_walkthrough.ipynb` | Saved entity lists and pathway enrichment, starting from a pairwise result |

## Running them

Each notebook has a single configuration cell near the top. Change the ids there and run the cells
in order; nothing else needs editing.

You will need an `MD_AUTH_TOKEN` in a `.env` file next to the notebook, or in your environment:

```
MD_AUTH_TOKEN=...
```

Beyond `md-python` itself, they need `python-dotenv`. `md_pairwise_ora_walkthrough.ipynb` also needs
`pandas` and `requests`, because it downloads a result table and decides significance locally:

```
pip install python-dotenv pandas requests
```

The notebooks are checked in without saved outputs, so you see your own results rather than someone
else's.

## What they create

All four write to the API. Between them they create uploads, datasets, analysis jobs, workspaces,
tabs, modules and entity lists in whichever organisation your token belongs to. Point them at a
development environment first, and delete what you no longer need.

## Two things worth knowing before you start

**Module settings are validated for shape, not for meaning.** The API checks that a settings hash
has the right keys and the right types, and returns a 422 when it does not. It does not check that a
value makes sense. A metadata column name that does not exist, or an enum value with the wrong
capitalisation, is accepted, stored, and only shows up later as an empty widget on a tab. The QC
report notebook builds a small validator for the checks you can make up front, and is explicit about
the ones you cannot.

**A required setting can still be empty.** Several modules declare a setting as required and give it
a default of `[]`. An empty list satisfies the requirement, so the module is created and then
renders nothing. Where that applies it is called out in the notebook, but it is the single easiest
way to build a report that looks broken for no visible reason.
