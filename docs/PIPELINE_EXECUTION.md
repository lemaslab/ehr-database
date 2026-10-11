# Running the EHR pipeline

[← Repository overview](../README.md) · [Data model](DATA_MODEL.md) · [Release QA](RELEASE_QC.md)

## Environment and prerequisites

The pipeline is an R project developed and tested on **Windows R 4.4.1 (ucrt)**. The processing scripts expect the repository working directory and access to approved, read-only raw EHR sources on the restricted `V:` drive. Check installed R packages used by each stage, including `dplyr`, `readr`, `lubridate`, `stringr`, `purrr`, `janitor`, and for the optional HTML report, `rmarkdown`/Pandoc. Package requirements are distributed across scripts rather than declared by a single lockfile.

Use the repository root as the working directory. The full runner is `code/run/99_run_all.R`; its eight stages stop on a blocking error.

## Safe local-only end-to-end run

From an **RStudio Console**:

```r
setwd("C:/Users/djlemas/Documents/GitHub/ehr-database")
stopifnot(file.exists("code/run/99_run_all.R"))
release_date <- "20261011" # Change deliberately for a new release

Sys.setenv(
  EHR_RELEASE_DATE = release_date,
  EHR_OUTPUT_MODE = "release",
  EHR_NETWORK_SYNC = "false",
  EHR_LOCAL_CSV = "false",
  EHR_NETWORK_CSV = "false",
  EHR_LAB_SOURCE_TAG = "20261010_193021",
  EHR_LAB_LINKAGE_DATE = release_date,
  EHR_LAB_COMBINE = "true",
  EHR_FORCE = "false",
  EHR_OVERWRITE_OUTPUTS = "false"
)
Sys.unsetenv("EHR_LAB_PILOT_N")
source("code/run/99_run_all.R")
```

**Important:** This is a safe configuration, **not** a command guaranteed to rerun against an existing same-date release. The shared writer **refuses to overwrite an existing same-date RDA** unless `EHR_OVERWRITE_OUTPUTS=true`. Choose a new release date for a new run, or back up existing artifacts and explicitly authorize replacement.

Before any *full rebuild of caches and same-date output files*, back up `data/cache/id_linkage_pipeline/` and the affected `data/processed/` release RDAs; then use `EHR_FORCE=true` and `EHR_OVERWRITE_OUTPUTS=true`. `EHR_FORCE` rebuilds the **linkage/delivery cache** (and forces lab postprocessing), not necessarily all intermediate computations. Reset both flags to `false` afterward. Do not assume previously published network files get updated by local-only reruns.

**Current script limitation:** Stage 08 in `99_run_all.R` sets its release-validation filenames using `Sys.Date()`, not exclusively `EHR_RELEASE_DATE`. For a backdated run or a run across midnight, inspect the version it will validate before execution. Do not claim the date environment variable currently pins *every* part of the pipeline.

## Stages and entry points

| Stage | Script | Primary action |
|---|---|---|
| 01 | [01_build_id_linkage.R](../code/run/01_build_id_linkage.R) | GNV/JAX mother–baby–delivery relationships |
| 02 | [02_build_delivery_encounter.R](../code/run/02_build_delivery_encounter.R) | Delivery encounters using the linkage |
| 03 | [03_validate_linkage.R](../code/run/03_validate_linkage.R) | Delivery/linkage diagnostics |
| 04 | [04_build_icd.R](../code/run/04_build_icd.R) | Maternal and infant diagnosis event tables |
| 05 | [05_build_medications.R](../code/run/05_build_medications.R) | Maternal IP and OP medication tables |
| 06 | [06_build_clinical_notes_metadata.R](../code/run/06_build_clinical_notes_metadata.R) | GNV/JAX note metadata and delivery timing |
| 07 | [07_link_and_release_labs.R](../code/run/07_link_and_release_labs.R) | Existing lab extracts linked to canonical IDs |
| 08 | [98_release_structured_and_notes.R](../code/run/98_release_structured_and_notes.R) | Release QC, manifest and report tables |

The separate [07_build_labs.R](../code/run/07_build_labs.R) is an **extract-production** script, not the Stage 07 executed by the master runner. See [Labs](LABORATORIES.md).

## Inspect results without rerunning

Each invocation creates `logs/run_YYYYMMDD_HHMMSS/` with a log per stage and `pipeline_execution.csv`. A successfully completed stage can still contain nonblocking warnings.

```r
runs <- sort(list.dirs("logs", recursive = FALSE), decreasing = TRUE)
runs <- runs[grepl("^run_", basename(runs))]
run_dir <- runs[[1]]
read.csv(file.path(run_dir, "pipeline_execution.csv"))
```

Run-scoped diagnostics may include `dataset_outputs.csv`, `schema_inventory.csv`, `identifier_qc.csv`, `substage_execution.csv`, `run_config.csv` and source inventory/read-call review files. Some are created only when their corresponding checks execute.

For a **standalone stage** called with `source(...)`, the master runner's stage log may not exist; messages can go only to the Console unless a run-log directory has been initialized.

## Error handling

Start troubleshooting at the failed stage `.log`, then review `pipeline_execution.csv`, relevant input paths, cache status, and any missing required columns. Avoid unnecessarily rerunning earlier large stages. Never use the master runner simply to regenerate the HTML report; consult [Release QA](RELEASE_QC.md).

## Publication to restricted network storage

Local validation and network publication are separate steps. The centralized writer supports `EHR_NETWORK_SYNC=true`, `EHR_NETWORK_CSV=true`, and `EHR_NETWORK_REQUIRED=true`. These should only be enabled after local QC and a reviewed destination. The writer itself always creates a **local RDA** first; network copies are verified with MD5. It does **not** provide a standalone sync-only invocation that automatically copies all existing RDAs, so blindly rerunning the entire pipeline just for network publication is not recommended.
