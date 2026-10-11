# Release manifest used by code/run/99_run_all.R.
# Produces aggregate operational metadata; no patient-level records.
ehr_release_manifest <- function(working_dir = getwd(), stage_log, run_id) {
  stopifnot(is.data.frame(stage_log),
            all(c("stage", "status", "seconds", "error") %in% names(stage_log)),
            length(run_id) == 1L, grepl("^[0-9]{8}_[0-9]{6}$", run_id))
  out <- file.path(working_dir, "reports", "output")
  dir.create(out, recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(stage_log, file.path(out, paste0("stages_", run_id, ".csv")),
                   row.names = FALSE)
  base <- file.path(working_dir, "data", "processed")
  f <- if (dir.exists(base)) list.files(base, recursive = TRUE,
       full.names = TRUE, pattern = "\\.(rda|rds|csv)$", ignore.case = TRUE) else character()
  f <- f[file.exists(f)]
  base_norm <- normalizePath(working_dir, winslash = "/", mustWork = TRUE)
  inventory <- data.frame(
    path = if (length(f)) substring(normalizePath(f, winslash = "/", mustWork = TRUE),
                                    nchar(base_norm) + 2L) else character(),
    bytes = if (length(f)) as.numeric(file.info(f)$size) else numeric(),
    modified_at = if (length(f)) as.character(file.info(f)$mtime) else character()
  )
  utils::write.csv(inventory, file.path(out, paste0("inventory_", run_id, ".csv")),
                   row.names = FALSE)
  manifest <- data.frame(
    run_id = run_id,
    generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    output_mode = Sys.getenv("EHR_OUTPUT_MODE", "release"),
    network_sync_requested = Sys.getenv("EHR_NETWORK_SYNC", "true"),
    stages_completed = sum(stage_log$status == "completed"),
    stages_failed = sum(stage_log$status == "failed"),
    local_files_in_inventory = nrow(inventory),
    inventory_includes_prior_outputs = TRUE
  )
  utils::write.csv(manifest, file.path(out, paste0("manifest_", run_id, ".csv")),
                   row.names = FALSE)
  message("Release manifest and inventory saved: ", out)
  message("Note: inventory includes previous artifacts; network copy is not verified here.")
  list(report_dir = out)
}
