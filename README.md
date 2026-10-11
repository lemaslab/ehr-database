# Maternal–Infant EHR Processing Pipeline

An R-based, reproducible processing and release workflow for maternal–infant electronic health records (EHR) from **UF Health Gainesville (GNV)** and **Jacksonville (JAX)**. The repository builds canonical mother–infant/delivery linkage, structured clinical datasets, longitudinal clinical-note **metadata**, and quality-controlled releases for perinatal research.

**Scope:** Code, documentation, source inventories, run diagnostics, and release QC—not the distribution of protected clinical data. Full clinical-note **text** and NLP/UFRC processing are managed separately in [lemaslab/nlp-perinatal](https://github.com/lemaslab/nlp-perinatal).

## Documentation

| Guide | Contents |
|---|---|
| **[Run the pipeline](docs/PIPELINE_EXECUTION.md)** | Prerequisites, local-only execution, caches, environment variables, stage order, troubleshooting |
| **[Data model and naming](docs/DATA_MODEL.md)** | Canonical IDs, dataset grains, directory conventions, file dates, provenance, data safety |
| **[Clinical-note metadata](docs/CLINICAL_NOTES.md)** | Prenatal/delivery/postnatal notes, linkage and timing, downstream UFRC text crosswalk |
| **[Laboratory integration](docs/LABORATORIES.md)** | Preserved source extracts versus ID-linked outputs, lab QC, safe reruns |
| **[Release QA and reporting](docs/RELEASE_QC.md)** | Validation checks, nine-dataset manifests, logs, checksums, HTML reporting |
| [Detailed lab release checklist](docs/LAB_RELEASE_INTEGRATION.md) | Historical and standalone lab execution details; confirm settings before use |
| [Programming governance prompt](documents/prompts/REPOSITORY_GOVERNANCE_PROMPT.md) | Consistency standards for coding assistants and contributors |

## Pipeline workflow

Entry point: **[`code/run/99_run_all.R`](code/run/99_run_all.R)**

| Stage | Processor | Primary output |
|---|---|---|
| 01 | Mother–infant/delivery linkage | `mom_baby_link_all_sites` |
| 02 | Delivery encounters | `delivery_encounter_all_sites` |
| 03 | Linkage and delivery quality checks | Structured QC |
| 04 | Maternal and infant ICD diagnoses | `mom_icd_all_sites`, `infant_icd_all_sites` |
| 05 | Maternal inpatient/outpatient medications | `mom_ip_all_sites`, `mom_op_all_sites` |
| 06 | Clinical-note metadata, linkage and timing | `clinical_notes_metadata_all_sites` |
| 07 | Canonical linkage of preserved lab extracts | `mom_labs_all_sites`, `infant_labs_all_sites` |
| 08 | Release validation and descriptive summaries | Manifest, QC checks, optional HTML |

The stage orchestration lives in [`code/run/`](code/run/), processing functions in [`code/functions/`](code/functions/), domain runners in [`code/process_data/`](code/process_data/), and reports in [`reports/`](reports/).

## Quick start (local only)

**Tested environment:** Windows R **4.4.1 ucrt**. Run from the repository root in RStudio with access to the approved restricted EHR source directories. Read the [execution guide](docs/PIPELINE_EXECUTION.md) before a first run or rebuild.

```r
setwd("C:/Users/djlemas/Documents/GitHub/ehr-database")

Sys.setenv(
  EHR_RELEASE_DATE = "20261011",       # Example: intentionally chosen release date
  EHR_OUTPUT_MODE = "release",
  EHR_NETWORK_SYNC = "false",          # Local-only; no shared-drive publication
  EHR_LOCAL_CSV = "false",
  EHR_NETWORK_CSV = "false",
  EHR_FORCE = "false",                 # Reuse eligible linkage/delivery caches
  EHR_OVERWRITE_OUTPUTS = "false",     # Refuse same-date output overwrite
  EHR_LAB_SOURCE_TAG = "20261010_193021", # Existing lab source extraction
  EHR_LAB_LINKAGE_DATE = "20261011",
  EHR_LAB_COMBINE = "true"
)
Sys.unsetenv("EHR_LAB_PILOT_N")

source("code/run/99_run_all.R")
```

**Do not rerun the example against an existing October 11 release without planning for existing files.** The writer refuses to overwrite same-date RDAs unless `EHR_OVERWRITE_OUTPUTS=true`. Back up matching output files and `data/cache/id_linkage_pipeline/` before enabling forced rebuilding with `EHR_FORCE=true`; disable force/overwrite afterward.

**Current limitation:** Stage 08 of the master runner selects its input dates using `Sys.Date()` rather than consistently using `EHR_RELEASE_DATE`. Backdated runs or executions spanning midnight need additional care. Standalone processors can also have distinct prerequisites—see the linked guides.

## Output organization

Canonical final RDA names use `<dataset>_YYYYMMDD.rda`. Examples:

```text
data/
  cache/id_linkage_pipeline/                  # Resumable linkage/delivery caches
  processed/
    GNV/mom_icd_gnv_YYYYMMDD.rda
    JAX/mom_icd_jax_YYYYMMDD.rda
    COMBINED/mom_baby_link_all_sites_YYYYMMDD.rda
    COMBINED/delivery_encounter_all_sites_YYYYMMDD.rda
    COMBINED/mom_icd_all_sites_YYYYMMDD.rda
    COMBINED/infant_icd_all_sites_YYYYMMDD.rda
    COMBINED/mom_ip_all_sites_YYYYMMDD.rda
    COMBINED/mom_op_all_sites_YYYYMMDD.rda
    COMBINED/clinical_notes_metadata_all_sites_YYYYMMDD.rda
    COMBINED/mom_labs_all_sites_YYYYMMDD.rda
    COMBINED/infant_labs_all_sites_YYYYMMDD.rda
    LAB_QC/lab_qc_YYYYMMDD.csv
    RELEASES/ehr_release_YYYYMMDD_HHMMSS/    # Manifest, QC, reports
logs/run_YYYYMMDD_HHMMSS/                    # Eight stage logs and diagnostics
```

A release manifest **references and checksums** the nine combined datasets; the release directory need not duplicate the underlying RDA files. Source laboratory RDAs are a special case: preserved extraction timestamps (e.g. `20261010_193021`) distinguish them from final date-only outputs.

## Identifiers and scientific interpretation

Core identifiers are `site`, `part_id_mom`, `part_id_infant`, and `delivery_id`. GNV participant IDs use `AC-mom-*`/`AC-infant-*`; JAX uses `DC-mom-*`/`DC-infant-*`. Note metadata uses `note_uid_global` for unique source-qualified note identity.

- Mother–infant linkage and delivery tables operate at infant grain; ICD, medication and laboratory tables preserve event/observation grain.
- Infant laboratory events can link to a canonical delivery; maternal laboratory events **remain mother-level**, without a validated pregnancy-specific delivery assignment.
- Note-to-delivery linkage retains explicit ambiguous cases. The UFRC text corpus requires an independently validated note-ID crosswalk; similarly named note keys are not automatically interchangeable.
- **No clinical-note text or patient-level EHR data should be committed to GitHub.**

## Release QC and run history

In a local end-to-end test on **2026-10-11**, the user reported eight completed stages and the following aggregate counts: **58,474 infants**, **44,964 mothers**, **57,000 delivery episodes**, **12,702,906 clinical-note metadata rows**, and **30,968,441 lab records matched to canonical patient identifiers**. Those are observed historical results, not permanent validation thresholds.

Review `logs/run_*/pipeline_execution.csv` and the release's `validation_summary.csv`/`manifest.csv` before accepting a dataset. The October 11 run also produced note-parser warnings and an optional HTML rendering warning; later report-path code changes still require a report-only local retest. The current validator contains a fixed historical note-count check, which should be revisited when the source corpus changes.

## Contributing

Use a feature branch and review changes for naming, cache handling, release-date pinning, provenance, and privacy. Follow the [repository governance prompt](documents/prompts/REPOSITORY_GOVERNANCE_PROMPT.md) and update the relevant section-level documentation when changing pipeline behavior. Restrict PHI/clinical text and patient-level exports to approved institutional storage.
