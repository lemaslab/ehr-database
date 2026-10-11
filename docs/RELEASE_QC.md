# Release validation, logs, and reports

[← Repository overview](../README.md) · [Pipeline execution](PIPELINE_EXECUTION.md) · [Data model](DATA_MODEL.md)

## Release artifacts

Stage 08 calls [98_release_structured_and_notes.R](../code/run/98_release_structured_and_notes.R) to validate the combined canonical files. The stage writes a directory similar to:

```text
data/processed/RELEASES/ehr_release_YYYYMMDD_HHMMSS/
  manifest.csv
  validation_summary.csv
  README.txt
  dataset_overview.csv
  site_summary.csv
  date_summary.csv
  category_summary.csv
  network_verification.csv
  ehr_release_report.html        # optional; may fail independently
```

Other source provenance inventories may also be present. The report pipeline is [08_build_release_report.R](../code/run/08_build_release_report.R), and the template is [reports/ehr_release_report.Rmd](../reports/ehr_release_report.Rmd). It summarizes data **without reproducing raw clinical-note text or patient-level records**.

Stage completion alone is not enough: inspect `validation_summary.csv` and `manifest.csv`. The release folder is a checksum-pinned **manifest of local RDA inputs**, not a duplicate of all nine source datasets.

## Inspect a release

```r
setwd("C:/Users/djlemas/Documents/GitHub/ehr-database")
release_dir <- "data/processed/RELEASES/ehr_release_20261011_140005" # Example
qc <- read.csv(file.path(release_dir,"validation_summary.csv"))
manifest <- read.csv(file.path(release_dir,"manifest.csv"))
print(qc[!qc$passed, , drop=FALSE])
stopifnot(all(qc$passed))
print(manifest[,c("dataset","source_file","md5")])
```

Normal full release validation expects the combined linkage, delivery, maternal/infant ICD, maternal IP/OP medication, note metadata, and (when `EHR_RELEASE_INCLUDE_LABS=true`) both combined lab files: **nine datasets**.

**Limitations of the current implementation:** the note-metadata QC checks a fixed 12,702,906-row historical count. New valid source cohorts may require updating this check to compare against a pinned source manifest rather than one fixed number. Lab matching is patient level; maternal pregnancy assignment is not provided.

## Stage logs

The master runner creates `logs/run_YYYYMMDD_HHMMSS/`, usually with:

- `01_linkage.log` through `08_release_qc.log`
- `pipeline_execution.csv` for stage status, runtime and errors
- `substage_execution.csv` for tracked ICD/medication substeps
- `dataset_outputs.csv`, `schema_inventory.csv`, `identifier_qc.csv`, `run_config.csv`, and source provenance reports, when available

Standalone stage runs are not automatically logged into an existing master run. Do not share operational logs without checking their paths and content for restricted information.

## HTML report without reprocessing data

HTML rendering is optional. An October 11, 2026 end-to-end run completed Stage 08 but produced an HTML rendering warning (`cannot open the connection`). A repository-root knitting fix was subsequently committed. For an independent report-only retry:

```r
setwd("C:/Users/djlemas/Documents/GitHub/ehr-database")
release_dir <- normalizePath(
  "data/processed/RELEASES/ehr_release_20261011_140005",
  winslash="/", mustWork=TRUE
)
rmarkdown::render(
  input="reports/ehr_release_report.Rmd",
  output_file="ehr_release_report.html",
  output_dir=release_dir,
  params=list(release_dir=release_dir),
  knit_root_dir=getwd(),
  quiet=FALSE
)
```

This requires existing release CSV summaries and a working `rmarkdown`/Pandoc installation; it should **not** rerun the underlying EHR stages.

## Network verification

The report's `network_verification.csv` checks whether expected network files exist, whether RDA MD5 values match, and whether CSV exports appear nonempty. A local PASS is independent of network publication. A network file being present is not proof of clinical correctness. Review data-transfer approvals and checksums before distributing a release.
