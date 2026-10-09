# Write dated local datasets and optionally synchronize identical copies to the shared drive.
# Existing callers can continue to use write_dataset(df, dataset_name, working_dir, subdir).
write_dataset <- function(df,
                          dataset_name,
                          working_dir = getwd(),
                          subdir = "COMBINED",
                          write_network = TRUE,
                          network_base = "V:/FACULTY/DJLEMAS/EHR_Data_processed") {
  if (!is.data.frame(df)) stop("df must be a data.frame")
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
    promoted <- file.rename(tmp, path)
    if (!promoted) {
      if (!is.null(backup)) file.rename(backup, path)
      stop("Cannot promote staged output: ", path)
    }
    if (!is.null(backup)) unlink(backup)
    invisible(path)
  }

  object_env <- new.env(parent = emptyenv())
  assign(dataset_name, df, envir = object_env)
  staged_write(rda_path, function(path) {
    save(list = dataset_name, file = path, envir = object_env)
  })
  staged_write(csv_path, function(path) readr::write_csv(df, path, na = ""))

  file_info <- function(path) {
    list(path = path, bytes = unname(file.info(path)$size),
         md5 = unname(tools::md5sum(path)))
  }
  local <- list(rda = file_info(rda_path), csv = file_info(csv_path))
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
        for (extension in c("rda", "csv")) {
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

  message("Saved locally: ", basename(rda_path), " and ", basename(csv_path))
  if (identical(network$status, "verified")) {
    message("Verified network copies: ", network_base)
  }

  # Backward-compatible rda/csv fields plus release-manifest metadata.
  invisible(list(
    rda = rda_path,
    csv = csv_path,
    dataset_name = dataset_name,
    rows = nrow(df),
    columns = ncol(df),
    local = local,
    network = network,
    written_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  ))
}
