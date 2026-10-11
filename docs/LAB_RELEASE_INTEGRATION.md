# Linked laboratory integration: release checklist

Source laboratory RDAs are the preserved, unlinked full extracts created on
2026-10-10 (timestamp 20261010_193021). They are not rebuilt by Stage 07.

## One-time linked laboratory postprocessing

In a fresh RStudio session at the repository root:

```r
Sys.setenv(
  EHR_LAB_SOURCE_TAG="20261010_193021",
  EHR_LAB_LINKAGE_DATE="20261010",
  EHR_OUTPUT_MODE="release",
  EHR_NETWORK_SYNC="true",
  EHR_LOCAL_CSV="false",
  EHR_NETWORK_CSV="true",
  EHR_NETWORK_REQUIRED="true"
)
source("code/run/07_link_and_release_labs.R")
```

Outputs are named `mom_labs_*_linked_YYYYMMDD.rda`,
`infant_labs_*_linked_YYYYMMDD.rda`, and combined
`mom_labs_all_sites_linked_YYYYMMDD.rda`,
`infant_labs_all_sites_linked_YYYYMMDD.rda`. The original six files
remain unchanged. Local outputs contain RDAs; network receives RDAs and CSVs.

The source timestamp must be updated if new original laboratory extracts are
created. A rerun skips existing same-date linked outputs; inspect network status
before concluding all outputs have synchronized.

## Include labs in independent release validation

```r
Sys.setenv(
  EHR_RELEASE_STRUCTURED_DATE="20261010",
  EHR_RELEASE_METADATA_FILE="clinical_notes_metadata_all_sites_20261010.rda",
  EHR_RELEASE_INCLUDE_LABS="true",
  EHR_RELEASE_LABS_DATE=format(Sys.Date(),"%Y%m%d")
)
source("code/run/98_release_structured_and_notes.R")
```

## Master pipeline

`code/run/99_run_all.R` now runs Stage 07 linked laboratories and Stage 08
release QC/HTML. It still runs stages 01–06 first, and requires the listed
original lab RDAs, complete source processors, and accessible network storage.
Do not run it merely to test the linked laboratory stage.

## Interpretation

A full 30,968,441-record lab-to-patient matching check on the original
extracts yielded 100% patient-ID matches across the four site/role groups.
Only matched infant lab records receive a delivery ID. Maternal records
remain at the mother level: pregnancy attribution needs temporal rules.
