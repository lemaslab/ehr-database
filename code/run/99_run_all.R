# Master EHR processing pipeline
#
# Runs the validated linkage foundation first, then downstream EHR datasets.
# Re-run safely after interruption: linkage/delivery stages reuse their caches
# unless EHR_FORCE=true.
#
# Force rebuild of cached linkage stages:
#   Sys.setenv(EHR_FORCE = "true")
#   source("code/run/99_run_all.R")
#   Sys.unsetenv("EHR_FORCE")

message("=== EHR FULL PROCESSING PIPELINE ===")

required_runners <- c(
  "code/run/00_pipeline_helpers.R",
  "code/run/01_build_id_linkage.R",
  "code/run/02_build_delivery_encounter.R",
  "code/run/03_validate_linkage.R",
  "code/run/04_build_icd.R",
  "code/run/05_build_medications.R",
  "code/run/06_build_clinical_notes_metadata.R",
  "code/process_data/run_mom_icd.R",
  "code/process_data/run_infant_icd.R",
  "code/process_data/run_mom_medications_ip.R",
  "code/process_data/run_mom_medications_op.R",
  "code/process_data/run_clinical_notes_metadata.R"
)

missing_runners <- required_runners[!file.exists(required_runners)]
if (length(missing_runners)) {
  stop(
    "Pipeline preflight failed. Missing required file(s):\n  ",
    paste(missing_runners, collapse = "\n  ")
  )
}

message("Stage 01: mother-infant-delivery linkage")
source("code/run/01_build_id_linkage.R")

message("Stage 02: delivery encounters")
source("code/run/02_build_delivery_encounter.R")

message("Stage 03: deep linkage QC")
source("code/run/03_validate_linkage.R")

message("Stage 04: maternal and infant ICD")
source("code/run/04_build_icd.R")

message("Stage 05: maternal medications")
source("code/run/05_build_medications.R")

message("Stage 06: clinical note metadata")
source("code/run/06_build_clinical_notes_metadata.R")

message("=== FULL EHR PIPELINE COMPLETE ===")
