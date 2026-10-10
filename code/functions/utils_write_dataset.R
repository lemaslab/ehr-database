# Write dated local datasets and optionally synchronize identical copies to the shared drive.
# Existing callers can continue to use write_dataset(df, dataset_name, working_dir, subdir).
write_dataset <- function(df,
                          dataset_name,
                          working_dir = getwd(),
                          subdir = "COMBINED",
                          write_network = TRUE,
                          network_base = "V:/FACULTY/DJLEMAS/EHR_Data_processed") {
  if (!is.data.frame(df)) stop("df must be a data.frame")
  mode <- tolower(Sys.getenv("EHR_OUTPUT_MODE", unset = "release"))
  if (!mode %in% c("pilot", "release")) stop("Invalid EHR_OUTPUT_MODE")
  if (mode == "pilot" || tolower(Sys.getenv("EHR_NETWORK_SYNC", unset = "true")) %in% c("false", "0", "no")) write_network <- FALSE
  if (exists("ehr_audit_schema", mode = "function")) ehr_audit_schema(df, dataset_name, "before_write", subdir)
  if (length(dataset_name) != 1L || is.na(dataset_name) ||
      !grepl("^[A-Za-z][A-Za-z0-9_.]*$", dataset_name)) {
    stop("dataset_name must be a valid R object name")
  }
  if (!requireNamespace("readr", quietly = TRUE)) stop("Package 'readr' is required")

  date_tag <- format(Sys.Date(), "%Y%m%d")
  file_base <- paste0(dataset_name, "_", date_tag)
  local_dir <- file.path(working_dir, "data", "processed", subdir)
  if (!dir.exists(local_dir) && !dir.create(local_dir, recursive = TRUE)) {
    stop("Cannot create local output directory: ", local_dir)
  }
  rda_path <- file.path(local_dir, paste0(file_base, ".rda"))
  csv_path <- file.path(local_dir, paste0(file_base, ".csv"))

  # Stage beside the destination; never delete a prior valid file before
  # the replacement has been fully written.
  staged_write <- function(path, writer) {
    tmp <- tempfile(pattern = paste0(".", basename(path), "_"),
                    tmpdir = dirname(path))
    on.exit(unlink(tmp), add = TRUE)
    writer(tmp)
    if (!file.exists(tmp) || is.na(file.info(tmp)$size) ||
        file.info(tmp)$size == 0) stop("Staged output is empty: ", path)
    # Windows does not reliably rename over an existing file. Keep a backup
    # and restore it if promotion fails.
    backup <- NULL
    if (file.exists(path)) {
      backup <- tempfile(pattern = paste0(".", basename(path), "_backup_"),
                         tmpdir = dirname(path))
      if (!file.rename(path, backup)) stop("Cannot back up existing output: ", path)
    }
    # On Windows/SMB, file.rename() can return FALSE even when the target
    # appears; fall back to verified copy, not to an unverified success.
    promoted <- suppressWarnings(file.rename(tmp, path))
    valid_target <- function() {
      file.exists(path) && isTRUE(file.info(path)$size > 0) &&
        identical(unname(tools::md5sum(tmp)), unname(tools::md5sum(path)))
    }
    if (!promoted && file.exists(tmp) && !file.exists(path)) {
      copied <- suppressWarnings(file.copy(tmp, path, overwrite = FALSE))
      promoted <- isTRUE(copied)
    }
    # If rename consumed tmp, the rename succeeded only if target exists.
    valid <- if (file.exists(tmp)) valid_target() else
      file.exists(path) && isTRUE(file.info(path)$size > 0)
    if (!valid) {
      # Restore the old file only when its destination is vacant.
      if (!is.null(backup) && file.exists(backup) && !file.exists(path)) {
        restored <- suppressWarnings(file.rename(backup, path))
        if (!restored) warning("Backup retained for manual recovery: ", backup)
      } else if (!is.null(backup) && file.exists(backup)) {
        warning("Backup retained for manual recovery: ", backup)
      }
      stop("Cannot verify staged output promotion: ", path)
    }
    if (!is.null(backup) && file.exists(backup)) unlink(backup)
    invisible(path)
  }

  object_env <- new.env(parent = emptyenv())
  assign(dataset_name, df, envir = object_env)
  staged_write(rda_path, function(path) {
    save(list = dataset_name, file = path, envir = object_env)
  })
  if (mode == "release") staged_write(csv_path, function(path) readr::write_csv(df, path, na = ""))

  file_info <- function(path) {
    list(path = path, bytes = unname(file.info(path)$size),
         md5 = unname(tools::md5sum(path)))
  }
  local <- list(rda = file_info(rda_path), csv = if (mode == "release") file_info(csv_path) else NULL)
  if (exists("ehr_log_append", mode = "function")) ehr_log_append("dataset_outputs.csv",
    list(dataset = dataset_name, mode = mode, rows = nrow(df), columns = ncol(df),
         rda_path = rda_path, rda_md5 = local$rda$md5))
  network <- list(status = if (write_network) "not_attempted" else "disabled",
                  rda = NULL, csv = NULL, error = NULL)

  # Network files are copied from the completed local files, rather than
  # serializing the dataset a second time. Hashes must match.
  if (write_network) {
    if (!dir.exists(network_base)) {
      network$status <- "unavailable"
      network$error <- paste("Network path not available:", network_base)
      warning(network$error)
    } else {
      tryCatch({
        network_dir <- file.path(network_base, subdir)
        if (!dir.exists(network_dir) &&
            !dir.create(network_dir, recursive = TRUE)) {
          stop("Cannot create network directory: ", network_dir)
        }
        for (extension in if (mode == "release") c("rda", "csv") else "rda") {
          src <- if (extension == "rda") rda_path else csv_path
          dst <- file.path(network_dir, basename(src))
          staged_write(dst, function(tmp) {
            if (!file.copy(src, tmp, overwrite = TRUE)) {
              stop("Network copy failed: ", dst)
            }
          })
          network[[extension]] <- file_info(dst)
          if (!identical(local[[extension]]$md5, network[[extension]]$md5)) {
            stop("Checksum mismatch for network copy: ", dst)
          }
        }
        network$status <- "verified"
      }, error = function(e) {
        network$status <<- "failed"
        network$error <<- conditionMessage(e)
        warning("Network synchronization failed: ", network$error)
      })
    }
  }

  message("Saved locally: ", basename(rda_path), if (mode == "release") paste0(" and ", basename(csv_path)) else " (pilot RDA only)")
  if (identical(network$status, "verified")) {
    message("Verified network copies: ", network_base)
  }

  # Backward-compatible rda/csv fields plus release-manifest metadata.
  invisible(list(
    rda = rda_path,
    csv = if (mode == "release") csv_path else NULL,
    dataset_name = dataset_name,
    rows = nrow(df),
    columns = ncol(df),
    local = local,
    network = network,
    written_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  ))
}
