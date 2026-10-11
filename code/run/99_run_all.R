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
  "code/run/00_raw_source_inventory.R",
  "code/run/07_release_manifest.R",
  "code/run/07_link_and_release_labs.R",
  "code/run/98_release_structured_and_notes.R",
  "code/run/01_build_id_linkage.R",
  "code/run/02_build_delivery_encounter.R",
  "code/run/03_validate_linkage.R",
  "code/run/04_build_icd.R",
  "code/run/05_build_medications.R",
  "code/run/06_build_clinical_notes_metadata.R",
  "code/process_data/run_mom_icd.R",
  "code/process_data/run_infant_icd.R",
  "code/process_data/run_mom_medications_IP.R",
  "code/process_data/run_mom_medications_OP.R",
  "code/process_data/run_clinical_notes_metadata.R"
)

missing_runners <- required_runners[!file.exists(required_runners)]
if (length(missing_runners)) {
  stop(
    "Pipeline preflight failed. Missing required file(s):\n  ",
    paste(missing_runners, collapse = "\n  ")
  )
}

source("code/run/00_pipeline_helpers.R")
run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_dir <- ehr_log_init(run_id)
source("code/run/00_raw_source_inventory.R",local=TRUE)
ehr_raw_source_inventory(root=getwd(),output_dir=log_dir)
utils::write.csv(ehr_variable_dictionary(),file.path(log_dir,"variable_dictionary.csv"),row.names=FALSE)
ehr_log_append("run_config.csv", list(run_id = run_id,
  output_mode = Sys.getenv("EHR_OUTPUT_MODE", "release"),
  network_sync = Sys.getenv("EHR_NETWORK_SYNC", "true"),
  local_csv = Sys.getenv("EHR_LOCAL_CSV","false"),
  network_csv = Sys.getenv("EHR_NETWORK_CSV","true"),
  network_required = Sys.getenv("EHR_NETWORK_REQUIRED","true"),
  started_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")))

stage_log <- data.frame(stage = character(), status = character(), seconds = numeric(),
                        error = character(), stringsAsFactors = FALSE)
run_stage <- function(name, script) {
  started <- Sys.time()
  status <- "completed"
  error <- ""
  log_file <- file.path(log_dir, paste0(name, ".log"))
  con <- file(log_file, open = "wt")
  sink(con, type = "output", split = TRUE)
  sink(con, type = "message")
  tryCatch(source(script, local = FALSE), error = function(e) {
    status <<- "failed"
    error <<- conditionMessage(e)
  })
  sink(type = "message")
  sink(type = "output")
  close(con)
  stage_log <<- rbind(stage_log, data.frame(
    stage = name, status = status,
    seconds = round(as.numeric(difftime(Sys.time(), started, units = "secs")), 1),
    error = error, stringsAsFactors = FALSE
  ))
  ehr_log_append("pipeline_execution.csv", list(stage = name, status = status,
    seconds = round(as.numeric(difftime(Sys.time(), started, units = "secs")), 1),
    error = error))
  if (status == "failed") stop("Stage ", name, " failed: ", error, call. = FALSE)
}
message("Stage 01: mother-infant-delivery linkage")
run_stage("01_linkage", "code/run/01_build_id_linkage.R")

message("Stage 02: delivery encounters")
run_stage("02_delivery", "code/run/02_build_delivery_encounter.R")

message("Stage 03: deep linkage QC")
run_stage("03_qc", "code/run/03_validate_linkage.R")

message("Stage 04: maternal and infant ICD")
run_stage("04_icd", "code/run/04_build_icd.R")

message("Stage 05: maternal medications")
run_stage("05_medications", "code/run/05_build_medications.R")

message("Stage 06: clinical note metadata")
run_stage("06_notes", "code/run/06_build_clinical_notes_metadata.R")

message("Stage 07: canonical laboratory linkage using preserved raw-extract RDAs")
run_stage("07_labs", "code/run/07_link_and_release_labs.R")

message("Stage 08: full release QC and descriptive HTML report")
old_settings <- Sys.getenv(
  c("EHR_RELEASE_STRUCTURED_DATE","EHR_RELEASE_METADATA_FILE",
    "EHR_RELEASE_INCLUDE_LABS","EHR_RELEASE_LABS_DATE"),
  unset=NA_character_)
restore_settings <- function() {
  for(i in seq_along(old_settings)) {
    key <- names(old_settings)[i]
    if(is.na(old_settings[i])) Sys.unsetenv(key)
    else do.call(Sys.setenv,as.list(stats::setNames(old_settings[i],key)))
  }
}
Sys.setenv(
  EHR_RELEASE_STRUCTURED_DATE=format(Sys.Date(),"%Y%m%d"),
  EHR_RELEASE_METADATA_FILE=paste0(
    "clinical_notes_metadata_all_sites_",format(Sys.Date(),"%Y%m%d"),".rda"),
  EHR_RELEASE_INCLUDE_LABS="true",
  EHR_RELEASE_LABS_DATE=format(Sys.Date(),"%Y%m%d")
)
tryCatch(
  run_stage("08_release_qc", "code/run/98_release_structured_and_notes.R"),
  finally=restore_settings()
)
writeLines(c(paste0("# EHR pipeline run ",run_id),
  paste0("Mode: ",Sys.getenv("EHR_OUTPUT_MODE","release")),
  paste0("Log directory: ",log_dir),
  paste0("Completed stages: ",sum(stage_log$status=="completed")),
  paste0("Failed stages: ",sum(stage_log$status=="failed"))),
  file.path(log_dir,"run_summary.md"))
message("=== FULL EHR PIPELINE COMPLETE ===")
