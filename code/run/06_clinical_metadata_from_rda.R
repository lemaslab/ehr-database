# Resume clinical-note metadata from a local RDA without rereading source CSVs.
# Usage: source("code/run/06_clinical_metadata_from_rda.R")
# Optional: Sys.setenv(EHR_METADATA_RDA="C:/path/to/input.rda")
# Never performs network synchronization. Retains original RDA unmodified.
local({
  root <- getwd()
  dir <- file.path(root, "data", "processed", "COMBINED")
  source("code/functions/link_clinical_notes_to_delivery.R")
  source("code/functions/add_clinical_note_timing.R")
  in_path <- Sys.getenv("EHR_METADATA_RDA", unset = "")
  if (!nzchar(in_path)) {
    candidates <- list.files(dir,
      pattern = "^clinical_notes_metadata_all_sites_[0-9]{8}\\.rda$",
      full.names = TRUE)
    if (!length(candidates)) stop("No local base metadata RDA found in ", dir)
    in_path <- sort(candidates, decreasing = TRUE)[1L]
  }
  if (!file.exists(in_path)) stop("Metadata input not found: ", in_path)
  e <- new.env(parent = emptyenv())
  load(in_path, envir = e)
  if (exists("clinical_notes_metadata", envir = e, inherits = FALSE)) {
    data <- get("clinical_notes_metadata", envir = e)
  } else if (exists("clinical_notes_metadata_all_sites", envir = e, inherits = FALSE)) {
    data <- get("clinical_notes_metadata_all_sites", envir = e)
  } else stop("Unrecognized metadata RDA object name: ", in_path)
  original_n <- nrow(data)
  if (!"delivery_id" %in% names(data))
    data <- link_clinical_notes_to_delivery(data, root)
  if (!"days_relative_delivery" %in% names(data))
    data <- add_clinical_note_timing(data, root)
  # Refresh flags from the centrally maintained timing function's rules.
  # For legacy timed objects, prefer regenerating from the base RDA so
  # all QC flags are derived consistently.
  if (!"timing_qc_status" %in% names(data))
    stop("Input already has timing but lacks QC flags. Choose a base metadata RDA.")
  stopifnot(
    nrow(data) == original_n,
    !anyNA(data$note_uid_global),
    anyDuplicated(data$note_uid_global) == 0L,
    !anyNA(data$timing_qc_status)
  )
  out <- file.path(dir, paste0("clinical_notes_metadata_all_sites_computed_",
    format(Sys.time(), "%Y%m%d_%H%M%S"), ".rda"))
  clinical_notes_metadata <- data
  save(clinical_notes_metadata, file = out, compress = TRUE)
  message("FINAL METADATA QC PASSED; saved local computed RDA: ", out)
  assign("clinical_notes_metadata", clinical_notes_metadata, envir = .GlobalEnv)
})
