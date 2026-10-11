# Export validated pilot RDA datasets to CSV without rerunning transformations.
# Example:
#   Sys.setenv(EHR_VALIDATED_RUN="20261009_185000")
#   source("code/run/98_export_csv_release.R")
# Requires manual approval of the specified pilot run's QC before invocation.
validated_run <- Sys.getenv("EHR_VALIDATED_RUN", "")
if (!nzchar(validated_run) || !grepl("^[0-9]{8}_[0-9]{6}$", validated_run))
  stop("Set EHR_VALIDATED_RUN to an explicitly QC-approved pilot run ID.")
root <- getwd()
pilot_dir <- file.path(root, "logs", paste0("run_", validated_run))
manifest <- file.path(pilot_dir, "dataset_outputs.csv")
if (!file.exists(manifest)) stop("Pilot output manifest not found: ", manifest)
outputs <- utils::read.csv(manifest, stringsAsFactors = FALSE)
outputs <- outputs[outputs$mode == "pilot", , drop = FALSE]
if (!nrow(outputs)) stop("No pilot RDA outputs found")
if (anyDuplicated(outputs$rda_path)) stop("Duplicate output paths in pilot manifest")
if (!requireNamespace("readr", quietly = TRUE)) stop("Install readr")
for (i in seq_len(nrow(outputs))) {
  path <- outputs$rda_path[i]
  if (!file.exists(path)) stop("Missing RDA: ", path)
  if (!identical(unname(tools::md5sum(path)), outputs$rda_md5[i]))
    stop("RDA checksum mismatch: ", path)
  env <- new.env(parent = emptyenv())
  loaded <- load(path, envir = env)
  expected <- outputs$dataset[i]
  if (!identical(loaded, expected)) stop("Unexpected RDA object name: ", path)
  df <- get(expected, envir = env, inherits = FALSE)
  if (!is.data.frame(df)) stop("Expected data frame in ", path)
  dst <- sub("\\.rda$", ".csv", path, ignore.case = TRUE)
  tmp <- tempfile(pattern = ".release_", tmpdir = dirname(dst))
  readr::write_csv(df, tmp, na = "")
  if (file.exists(dst)) stop("CSV already exists; refusing overwrite: ", dst)
  if (!file.rename(tmp, dst)) stop("Could not publish CSV: ", dst)
  message("Exported ", dst)
}
message("CSV export complete. Network publication is a separate release step.")
