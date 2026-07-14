# MD format — Metabolomics (`md_format_metabolite`)

How to prepare a metabolomics dataset for upload to Mass Dynamics. This is the
metabolite-level variant of MD format; upload it with
`source="md_format_metabolite"`.

The files in this folder are a complete, working example you can copy and adapt:

| File | Purpose |
|------|---------|
| `example_md_format_metabolite.tsv` | The intensity data file (the one that matters) |
| `experiment_design.csv` | **Required** companion — maps each sample to its condition |
| `sample_metadata.csv` | **Required** companion — any per-sample annotations |

---

## 1. The data file

MD format is **long format**: one row per **metabolite × sample**. A wide
intensity matrix (metabolites in rows, samples in columns) must be *melted*
into this shape first.

### Required columns

| Column | Type | Rule |
|--------|------|------|
| `MetaboliteId` | string | The metabolite identifier — use a **stable, unique** id such as an **InChIKey** (recommended), HMDB or KEGG ID. This is the key that groups rows into one metabolite, so it must be unique per metabolite. The example uses InChIKeys. |
| `MetaboliteIntensity` | float | The measured intensity. Use `0.0` for a missing measurement (see Imputed). |
| `SampleName` | string | Sample identifier. **Must match `sample_name` in the companion files exactly** (case-sensitive). |
| `Imputed` | integer `0` or `1` | `1` = this value is not a real measurement; `0` = real measurement. **Required and validated** — any other value is rejected. |

> **Tip — human-readable names.** Because `MetaboliteId` is best set to a stable
> id like an InChIKey, add a `MetaboliteName` column (a pass-through metadata
> column, see below) carrying the common name so plots and tables are readable.
> The example does exactly this.

> **The Imputed rule (most common mistake).** Every row where
> `MetaboliteIntensity` is `0.0` **must** have `Imputed = 1`. A zero left at
> `Imputed = 0` is treated as a genuine measured value and will corrupt
> downstream statistics. Unlike the gene format, metabolite `Imputed` is **not**
> auto-derived — you set it yourself.

### Full-matrix requirement

The file must be a **complete matrix**: every `MetaboliteId × SampleName`
combination present as exactly one row. A non-measurement is a row with
intensity `0.0` and `Imputed = 1` — never an absent row. (If you melt a
complete wide matrix, this is automatic.)

So for *M* metabolites and *S* samples you have exactly *M × S* data rows. The
example has 3 metabolites × 4 samples = 12 rows.

---

## 2. Extra metadata columns — add as many as you like

**Any column beyond the four required ones is carried through automatically** and
attached to the metabolite in Mass Dynamics. You don't register it anywhere —
just include it. Useful descriptors:

`MetaboliteName` (common name), `Description`, `KEGG`, `HMDB`, `m/z`,
`RetentionTime`, `formula`, `SMILES`, `pathway`, …

**The one rule:** a metadata column must hold **one value per `MetaboliteId`** —
i.e. it is constant across all of that metabolite's sample rows. md-converter
validates this and **rejects** a metadata column whose value varies within a
single metabolite (that's a per-sample measurement, not metabolite metadata, and
belongs in `sample_metadata.csv` instead).

- Unknown value? Leave it blank — just keep it blank consistently for that
  metabolite.
- `MetaboliteIntensity` and `Imputed` are the only columns allowed to vary
  per sample.

In the example, `MetaboliteName` and `Description` are pass-through metadata:
notice they repeat identically across each metabolite's four rows, while the
`MetaboliteId` itself is the InChIKey.

---

## 3. Example data file

```
MetaboliteId	MetaboliteName	Description	SampleName	MetaboliteIntensity	Imputed
FHQVHHIBKUMWTI-OUTUXVNYSA-N	LysoPE(18:2)	Lysophosphatidylethanolamine	C115_N	740.602952	0
FHQVHHIBKUMWTI-OUTUXVNYSA-N	LysoPE(18:2)	Lysophosphatidylethanolamine	C24_N	1798.107995	0
WQZGKKKJIJFFOK-GASJEMHNSA-N	Glucose	D-Glucose	C115_N	15320.5	0
WQZGKKKJIJFFOK-GASJEMHNSA-N	Glucose	D-Glucose	C24_N	0.0	1
KRKNYBCHXYNGOX-UHFFFAOYSA-N	Citrate	Citric acid	C115_N	8421.33	0
KRKNYBCHXYNGOX-UHFFFAOYSA-N	Citrate	Citric acid	C24_N	9105.87	0
```

Reading the rows:
- `MetaboliteId` is the InChIKey — a stable, unique identifier per metabolite.
- `MetaboliteName` / `Description` are constant within each metabolite → valid pass-through metadata (the readable name lives here).
- The `Glucose / C24_N` row is a missing measurement → `0.0` with `Imputed = 1`.
- Every metabolite appears once per sample → full matrix.

---

## 4. Companion files (both required)

A metabolite upload also needs an **`experiment_design.csv`** and a
**`sample_metadata.csv`**. (Only the gene format is exempt from
`experiment_design`; metabolite is not.)

`experiment_design.csv` — minimal, maps sample → condition:

```
sample_name,condition
C115_N,Normal
C24_N,Normal
T10_D,Disease
T22_D,Disease
```

`sample_metadata.csv` — any per-sample annotations (drives grouping/colouring in
plots). Put **per-sample** variables here (gender, batch, age, timepoint…) —
this is the home for anything that varies across samples:

```
sample_name,condition,gender,batch
C115_N,Normal,F,1
C24_N,Normal,M,1
T10_D,Disease,F,2
T22_D,Disease,M,2
```

`sample_name` values in **all three files** must match exactly (case-sensitive).

---

## 5. Converting a wide matrix → md_format_metabolite (pandas)

If your data is a wide matrix (metabolites in rows, one column per sample):

```python
import pandas as pd

# wide: index/first cols = metabolite id (+ any descriptors), remaining cols = samples
wide = pd.read_csv("my_metabolomics_wide.tsv", sep="\t")

id_col = "MetaboliteId"                       # an InChIKey / HMDB / KEGG id
meta_cols = ["MetaboliteName", "Description"]  # any descriptors to carry through
sample_cols = [c for c in wide.columns if c not in [id_col, *meta_cols]]

long_df = wide.melt(
    id_vars=[id_col, *meta_cols],
    value_vars=sample_cols,
    var_name="SampleName",
    value_name="MetaboliteIntensity",
)

# missing measurements -> 0.0 + Imputed = 1
long_df["MetaboliteIntensity"] = long_df["MetaboliteIntensity"].fillna(0.0)
long_df["Imputed"] = (long_df["MetaboliteIntensity"] == 0).astype(int)

long_df.to_csv("example_md_format_metabolite.tsv", sep="\t", index=False)
```

This melt produces a full matrix automatically and keeps `meta_cols` as
pass-through metadata (constant per metabolite by construction).

---

## 6. Upload

With the [md-python](../../README.md) client / MCP server:

```python
client.uploads.create(
    source="md_format_metabolite",
    filenames=["example_md_format_metabolite.tsv"],
    experiment_design="experiment_design.csv",
    sample_metadata="sample_metadata.csv",
    # ...name / other args per your client setup
)
```

From the MCP server, the same fields are collected by `create_upload`
(`source="md_format_metabolite"`). You can also call `get_md_format_spec("metabolite")`
to get this schema, the conversion template, and this example back as JSON.

---

## Checklist before uploading

- [ ] Columns `MetaboliteId`, `MetaboliteIntensity`, `SampleName`, `Imputed` all present
- [ ] Every `MetaboliteIntensity == 0.0` row has `Imputed == 1`
- [ ] `Imputed` is only ever `0` or `1`
- [ ] Full matrix: rows == (#metabolites × #samples), one row per combination
- [ ] Extra metadata columns are constant within each `MetaboliteId`
- [ ] `SampleName` matches `sample_name` in `experiment_design.csv` and `sample_metadata.csv` exactly
- [ ] `experiment_design.csv` and `sample_metadata.csv` included
