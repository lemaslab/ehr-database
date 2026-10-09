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

source("code/run/00_pipeline_helpers.R")
run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_dir <- ehr_log_init(run_id)
ehr_log_append("run_config.csv", list(run_id = run_id,
  output_mode = Sys.getenv("EHR_OUTPUT_MODE", "release"),
  network_sync = Sys.getenv("EHR_NETWORK_SYNC", "true"),
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

message("Stage 07: release manifest and HTML report")
source("code/run/07_release_manifest.R")
release <- ehr_release_manifest(getwd(), stage_log, run_id)
if (requireNamespace("rmarkdown", quietly = TRUE)) {
  tryCatch(
    rmarkdown::render("reports/ehr_pipeline_report.Rmd",
      output_file = paste0("ehr_pipeline_", run_id, ".html"),
      output_dir = release$report_dir,
      params = list(run_id = run_id),
      envir = new.env(parent = globalenv()), quiet = TRUE),
    error = function(e) warning("HTML report failed: ", conditionMessage(e))
  )
} else {
  warning("Install rmarkdown to render the HTML report; CSV manifest was saved.")
}
writeLines(c(paste0("# EHR pipeline run ", run_id),
  paste0("Mode: ", Sys.getenv("EHR_OUTPUT_MODE", "release")),
  paste0("Log directory: ", log_dir),
  paste0("Completed stages: ", sum(stage_log$status == "completed")),
  paste0("Failed stages: ", sum(stage_log$status == "failed"))),
  file.path(log_dir, "run_summary.md"))
message("=== FULL EHR PIPELINE COMPLETE ===")
