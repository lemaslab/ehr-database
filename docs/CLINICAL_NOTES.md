# Clinical-note metadata and downstream UFRC text

[← Repository overview](../README.md) · [Data model](DATA_MODEL.md) · [Release QA](RELEASE_QC.md)

## What this repository does

Stage 06 calls [06_build_clinical_notes_metadata.R](../code/run/06_build_clinical_notes_metadata.R) and [run_clinical_notes_metadata.R](../code/process_data/run_clinical_notes_metadata.R) to assemble metadata from GNV/JAX **prenatal, delivery, and postnatal** sources, covering maternal and postnatal infant notes.

It standardizes source note IDs, site, participant role, note period, provenance, and `note_uid_global`. It then calls:

- [link_clinical_notes_to_delivery.R](../code/functions/link_clinical_notes_to_delivery.R) for canonical delivery association, including explicit ambiguous/unmatched classifications.
- [add_clinical_note_timing.R](../code/functions/add_clinical_note_timing.R) for delivery-relative timing and postpartum windows.

The result is a **metadata table** and does **not** contain a validated full-text clinical-note corpus.

## Expected note variables

Common metadata includes `part_id`, `part_id_mom`, `part_id_infant`, `note_uid`, `note_uid_global`, `note_id_source`, `note_created_datetime`, `site`, `note_period`, `participant_role`, `source_group`, `source_file`, `delivery_id`, `delivery_link_status`, `timing_qc_status`, and delivery-relative date/window variables.

`note_uid` may repeat across source files. Treat **`note_uid_global`** as the intended globally unique note key within this release. Timing classification does not mean an ambiguous mother-to-delivery relation has become certain.

## Last observed local integration test

On 2026-10-11, a user-run eight-stage local pipeline reported **12,702,906** metadata rows (both sites) and zero duplicated `note_uid_global` identifiers during metadata QC. This is a **historical test result**, not a permanent expected size for future source releases.

Some source files emitted a `readr` warning about a `Deidentified_note_key` parser not matching available column names. This should be audited at the source-specific parser level, even when other metadata checks pass.

## Integration with the UFRC text corpus

The companion repository, [lemaslab/nlp-perinatal](https://github.com/lemaslab/nlp-perinatal), contains UFRC SLURM/Parquet note-text processing and NLP annotation/phenotyping workflows. These are **separate processing systems**. Never attach mother/infant IDs to raw text by an unverified string match. Establish a candidate crosswalk from source identifiers and provenance; confirm key uniqueness, match cardinality, unmatched rates, and manual spot checks before creating a linked text dataset. Retain uncertain matches for review.

## How to execute

Run Stage 06 as part of [99_run_all.R](../code/run/99_run_all.R), or, after building the matching canonical linkage and delivery files, execute:

```r
setwd("C:/Users/djlemas/Documents/GitHub/ehr-database")
Sys.setenv(EHR_OUTPUT_MODE="release", EHR_NETWORK_SYNC="false")
source("code/run/06_build_clinical_notes_metadata.R")
```

Standalone execution can be memory-intensive. Follow the script's `debug_mode` and `write_outputs` switches for troubleshooting; do not use a debug subset as a production corpus.
