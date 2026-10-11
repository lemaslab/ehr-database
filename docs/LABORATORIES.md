# Laboratory processing and linkage

[← Repository overview](../README.md) · [Existing detailed lab checklist](LAB_RELEASE_INTEGRATION.md) · [Release QA](RELEASE_QC.md)

## Two different lab steps

1. **Source extraction** — [07_build_labs.R](../code/run/07_build_labs.R) plus [process_structured_labs.R](../code/functions/process_structured_labs.R) standardizes raw laboratory source CSVs into preserved, timestamped source RDAs. This is not normally needed on every pipeline run.
2. **Canonical linkage and release** — [07_link_and_release_labs.R](../code/run/07_link_and_release_labs.R) loads the existing site/role source RDAs, verifies lab patient identifiers against the selected canonical linkage, retains unmatched rows with status labels, and writes four dated site-specific and two combined RDA datasets.

The master pipeline **Stage 07 is step 2**, not step 1. Avoid regenerating 31 million source lab events when the existing extracts are intact.

## Source variables and linkage rules

The processed observation schema includes `lab_participant_id`, `specimen_datetime`, `lab_result_raw`, and `loinc_code`; the original source identity and available lab descriptions/results should remain auditable in the source extracts. `lab_link_status` classifies verification status.

Infant observations may receive `delivery_id` through the one-to-one infant linkage. **Maternal lab observations stay at the mother level with `delivery_id=NA`**; pregnancy-specific assignment is a separate methodology and is not performed here.

## Run locally from preserved extracts

```r
setwd("C:/Users/djlemas/Documents/GitHub/ehr-database")
Sys.setenv(
  EHR_RELEASE_DATE="20261011",
  EHR_LAB_SOURCE_TAG="20261010_193021",
  EHR_LAB_LINKAGE_DATE="20261011",
  EHR_OUTPUT_MODE="release",
  EHR_NETWORK_SYNC="false",
  EHR_LOCAL_CSV="false",
  EHR_NETWORK_CSV="false",
  EHR_LAB_COMBINE="true"
)
Sys.unsetenv("EHR_LAB_PILOT_N")
source("code/run/07_link_and_release_labs.R")
```

This **requires** `data/processed/COMBINED/mom_baby_link_all_sites_20261011.rda` and the original timestamped lab RDAs. If same-date canonical lab outputs already exist, the script may skip them unless `EHR_FORCE=true`, and the writer independently requires `EHR_OVERWRITE_OUTPUTS=true` for same-date replacements. Back up old outputs first; do not assume skipped outputs have been refreshed or network-synchronized.

## Observed historical QC

An October 11, 2026 local run reported:

| Source group | Records | Canonical patient-ID matches |
|---|---:|---:|
| Maternal GNV | 9,142,610 | 9,142,610 |
| Maternal JAX | 15,236,243 | 15,236,243 |
| Infant GNV | 4,354,358 | 4,354,358 |
| Infant JAX | 2,235,230 | 2,235,230 |

The 100% match is *patient-level* evidence for the selected linkage; it is not proof of pregnancy-specific attribution or of new source-release comparability.

QC CSV: `data/processed/LAB_QC/lab_qc_YYYYMMDD.csv`. For further implementation detail, see [LAB_RELEASE_INTEGRATION.md](LAB_RELEASE_INTEGRATION.md).
