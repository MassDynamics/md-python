# Dataset Output Types

| Type | What it contains | Native viz |
|---|---|---|
| INTENSITY | Intensity matrix + metadata | Heatmap, PCA, box plot, QC plots |
| PAIRWISE | DE results (protein, log2fc, pvalue) | Volcano plot |
| ANOVA | Multi-group test results | ANOVA volcano |
| ENRICHMENT | Pathway analysis results | Reactome strip |
| DOSE_RESPONSE | Curve fit results | Dose-response curves |
| NORMALISATION_AND_IMPUTATION | Normalised + imputed matrix | (Internal) |
| DOSE_RESPONSE_AGGREGATE | Aggregated DR results | (Internal) |

The viz service uses the output type to determine which plots can render the
results. If your analysis produces PAIRWISE output, volcano plots work
automatically.
