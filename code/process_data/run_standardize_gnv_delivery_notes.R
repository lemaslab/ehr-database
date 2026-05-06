# ============================================================
# Stand-alone script: Standardize GNV Delivery Notes
# ============================================================
# Default is troubleshooting mode: process one zip batch only.

library(dplyr)
library(readr)
library(stringr)
library(purrr)
library(tibble)
library(fs)

# ============================================================
# 00. Troubleshooting controls
# ============================================================

troubleshoot_mode <- TRUE
test_batch_n <- 1
test_batch_pattern <- NA_character_
max_notes_to_read <- Inf
force_reunzip <- TRUE

# ============================================================
# 01. Locate repo root
# ============================================================

possible_roots <- c(
  getwd(),
  "C:/Users/djlemas/Documents/GitHub/ehr-database",
  "C:/Users/djlemas/OneDrive/Documents/ehr-database"
)

working_dir <- NULL
for (p in possible_roots) {
  if (dir.exists(file.path(p, "code", "functions"))) {
    working_dir <- normalizePath(p, winslash = "/", mustWork = TRUE)
    break
  }
}
if (is.null(working_dir)) stop("Could not locate repo root.")
message("Using working_dir: ", working_dir)

# ============================================================
# 02. Paths and logging
# ============================================================

notes_dir <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_10_2021/delivery_notes"
if (!dir.exists(notes_dir)) stop("notes_dir does not exist: ", notes_dir)

date_tag <- format(Sys.Date(), "%Y%m%d")
run_label <- if (troubleshoot_mode) paste0("troubleshoot_batch", test_batch_n) else "all_batches"

output_dir <- file.path(working_dir, "data", "processed", "notes")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

extract_dir <- file.path(output_dir, "gnv_delivery_notes_unzipped", run_label)
dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)

log_dir <- file.path(working_dir, "logs")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
log_file <- file.path(log_dir, paste0("standardize_gnv_delivery_notes_", run_label, "_", date_tag, ".log"))

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
sink(log_con, type = "message")
on.exit({ sink(type = "message"); sink(); close(log_con) }, add = TRUE)

message("==== STANDARDIZE GNV DELIVERY NOTES ====")
message("Run time: ", Sys.time())
message("Troubleshoot mode: ", troubleshoot_mode)
message("Selected test batch number: ", test_batch_n)
message("Selected test batch pattern: ", test_batch_pattern)
message("Max notes to read: ", max_notes_to_read)
message("force_reunzip: ", force_reunzip)
message("notes_dir: ", notes_dir)
message("output_dir: ", output_dir)
message("extract_dir: ", extract_dir)
message("log_file: ", log_file)

# ============================================================
# 03. Robust unzip helper
# ============================================================

robust_unzip <- function(zip_file, exdir) {
  zip_file <- normalizePath(zip_file, winslash = "\\", mustWork = TRUE)
  exdir <- normalizePath(exdir, winslash = "\\", mustWork = FALSE)
  dir.create(exdir, recursive = TRUE, showWarnings = FALSE)

  message("Testing zip listing with utils::unzip(list = TRUE): ", zip_file)
  zip_list <- tryCatch(
    utils::unzip(zip_file, list = TRUE),
    error = function(e) {
      warning("utils::unzip(list = TRUE) failed: ", conditionMessage(e))
      NULL
    }
  )

  if (!is.null(zip_list)) {
    message("Files listed in zip: ", nrow(zip_list))
    print(head(zip_list, 20))
  }

  message("Trying utils::unzip extraction...")
  unzip_ok <- tryCatch({
    utils::unzip(zip_file, exdir = exdir)
    TRUE
  }, warning = function(w) {
    warning("utils::unzip warning: ", conditionMessage(w))
    FALSE
  }, error = function(e) {
    warning("utils::unzip error: ", conditionMessage(e))
    FALSE
  })

  files_after <- dir(exdir, recursive = TRUE, full.names = TRUE, all.files = FALSE)
  if (unzip_ok && length(files_after) > 0) {
    message("utils::unzip succeeded. Files extracted: ", length(files_after))
    return(TRUE)
  }

  message("utils::unzip did not extract files. Trying PowerShell Expand-Archive fallback...")

  ps_cmd <- paste0(
    "Expand-Archive -LiteralPath ", shQuote(zip_file, type = "cmd"),
    " -DestinationPath ", shQuote(exdir, type = "cmd"),
    " -Force"
  )

  ps_status <- system2("powershell", args = c("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd))

  files_after_ps <- dir(exdir, recursive = TRUE, full.names = TRUE, all.files = FALSE)

  if (ps_status == 0 && length(files_after_ps) > 0) {
    message("PowerShell Expand-Archive succeeded. Files extracted: ", length(files_after_ps))
    return(TRUE)
  }

  stop(
    "Could not unzip file with utils::unzip or PowerShell Expand-Archive: ", zip_file,
    "\nTry manually extracting this batch to: ", exdir
  )
}

# ============================================================
# 04. Locate/select zip batches
# ============================================================

zip_files_all <- dir_ls(notes_dir, regexp = "\\.zip$", recurse = FALSE, type = "file") %>% sort()
message("Total zip batches found: ", length(zip_files_all))
print(zip_files_all)
if (length(zip_files_all) == 0) stop("No .zip files found in notes_dir: ", notes_dir)
if (length(zip_files_all) != 6) warning("Expected 6 zip batches, but found ", length(zip_files_all), ". Continuing.")

zip_inventory <- tibble(
  batch_number = seq_along(zip_files_all),
  zip_file = as.character(zip_files_all),
  zip_name = path_file(zip_files_all),
  zip_size_bytes = as.numeric(file_size(zip_files_all))
)
message("Zip inventory:")
print(zip_inventory)

if (troubleshoot_mode) {
  if (!is.na(test_batch_pattern)) {
    zip_files <- zip_files_all[str_detect(path_file(zip_files_all), fixed(test_batch_pattern, ignore_case = TRUE))]
    if (length(zip_files) == 0) stop("No zip file matched test_batch_pattern: ", test_batch_pattern)
    if (length(zip_files) > 1) zip_files <- zip_files[1]
  } else {
    if (test_batch_n < 1 || test_batch_n > length(zip_files_all)) stop("test_batch_n must be between 1 and ", length(zip_files_all))
    zip_files <- zip_files_all[test_batch_n]
  }
} else {
  zip_files <- zip_files_all
}
message("Zip files selected for this run: ", length(zip_files))
print(zip_files)

# ============================================================
# 05. Unzip selected batch/batches
# ============================================================

if (force_reunzip && dir.exists(extract_dir)) {
  message("force_reunzip is TRUE. Removing extract_dir: ", extract_dir)
  unlink(extract_dir, recursive = TRUE, force = TRUE)
  dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)
}

for (zip_file in zip_files) {
  batch_name <- path_ext_remove(path_file(zip_file))
  batch_extract_dir <- file.path(extract_dir, batch_name)
  dir.create(batch_extract_dir, recursive = TRUE, showWarnings = FALSE)
  message("Unzipping: ", path_file(zip_file))
  robust_unzip(zip_file, batch_extract_dir)
}

# ============================================================
# 06. Inventory extracted note files
# ============================================================

note_files <- dir_ls(extract_dir, recurse = TRUE, type = "file")
if (length(note_files) == 0) stop("No files found after unzipping to: ", extract_dir)

note_inventory <- tibble(
  site = "GNV",
  note_set = "delivery_notes",
  note_type = "delivery",
  troubleshoot_mode = troubleshoot_mode,
  run_label = run_label,
  note_file_path = as.character(note_files),
  note_file = path_file(note_files),
  batch_zip = path_file(path_dir(note_files)),
  file_ext = str_to_lower(path_ext(note_files)),
  file_size_bytes = as.numeric(file_size(note_files))
)

message("Extracted note files: ", nrow(note_inventory))
message("Inventory by batch and extension:")
print(note_inventory %>% count(batch_zip, file_ext, name = "n_files"))
message("First 50 extracted filenames:")
print(head(note_inventory$note_file, 50))

note_inventory_to_read <- if (is.finite(max_notes_to_read)) {
  note_inventory %>% slice_head(n = max_notes_to_read)
} else {
  note_inventory
}
message("Notes selected for text reading: ", nrow(note_inventory_to_read))

# ============================================================
# 07. Read note text
# ============================================================

read_note_safely <- function(path) {
  tryCatch(
    paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n"),
    error = function(e) {
      warning("Could not read file: ", path, " | ", conditionMessage(e))
      NA_character_
    }
  )
}

notes_raw <- note_inventory_to_read %>%
  mutate(
    note_text = map_chr(note_file_path, read_note_safely),
    note_nchar = nchar(note_text),
    note_nlines = if_else(is.na(note_text), NA_integer_, str_count(note_text, "\n") + 1L),
    note_text_preview = str_sub(str_squish(note_text), 1, 500)
  )

message("Text read QC:")
print(notes_raw %>% summarise(
  n_files_read = n(),
  n_missing_text = sum(is.na(note_text) | note_text == ""),
  min_nchar = suppressWarnings(min(note_nchar, na.rm = TRUE)),
  median_nchar = suppressWarnings(median(note_nchar, na.rm = TRUE)),
  max_nchar = suppressWarnings(max(note_nchar, na.rm = TRUE))
))
message("First 5 note text previews:")
print(notes_raw %>% select(note_file, note_text_preview) %>% head(5))

# ============================================================
# 08. Find optional metadata file
# ============================================================

metadata_path <- NA_character_
candidate_metadata <- dir_ls(
  notes_dir,
  recurse = FALSE,
  type = "file",
  regexp = "(?i)(metadata|meta|index|note).*\\.(csv|rds|rda|RData)$"
)
message("Candidate metadata files found: ", length(candidate_metadata))
print(candidate_metadata)

if (length(candidate_metadata) == 1) {
  metadata_path <- as.character(candidate_metadata)
  message("Using metadata file: ", metadata_path)
} else if (length(candidate_metadata) > 1) {
  warning("Multiple candidate metadata files found. Set metadata_path manually after reviewing candidates.")
} else {
  warning("No candidate metadata file found. Continuing without metadata join.")
}

read_metadata_safely <- function(path) {
  if (is.na(path) || !file.exists(path)) return(NULL)
  ext <- str_to_lower(path_ext(path))
  if (ext == "csv") return(read_csv(path, show_col_types = FALSE))
  if (ext == "rds") return(readRDS(path))
  if (ext %in% c("rda", "rdata")) {
    env <- new.env(parent = emptyenv())
    loaded_names <- load(path, envir = env)
    if (length(loaded_names) == 0) return(NULL)
    return(env[[loaded_names[1]]])
  }
  NULL
}
metadata_df <- read_metadata_safely(metadata_path)
if (!is.null(metadata_df)) {
  metadata_df <- as_tibble(metadata_df)
  message("Metadata rows: ", nrow(metadata_df))
  message("Metadata columns:")
  print(names(metadata_df))
} else {
  message("No metadata loaded.")
}

# ============================================================
# 09. Parse, link, standardize
# ============================================================

notes_parsed <- notes_raw %>%
  mutate(
    note_file_stem = path_ext_remove(note_file),
    date_yyyymmdd = str_extract(note_file, "(?<!\\d)(19|20)\\d{6}(?!\\d)"),
    date_yyyy_mm_dd = str_extract(note_file, "(?<!\\d)(19|20)\\d{2}[-_]\\d{2}[-_]\\d{2}(?!\\d)"),
    note_date_raw = coalesce(date_yyyy_mm_dd, date_yyyymmdd),
    note_date = case_when(
      !is.na(date_yyyy_mm_dd) ~ as.Date(str_replace_all(date_yyyy_mm_dd, "_", "-")),
      !is.na(date_yyyymmdd) ~ as.Date(date_yyyymmdd, format = "%Y%m%d"),
      TRUE ~ as.Date(NA)
    ),
    mom_id_raw = str_extract(note_file, "(?i)(mom|mother|maternal)[_-]?[A-Za-z0-9]+"),
    infant_id_raw = str_extract(note_file, "(?i)(baby|infant|child|newborn)[_-]?[A-Za-z0-9]+"),
    first_numeric_id = str_extract(note_file, "(?<!\\d)\\d{4,}(?!\\d)")
  )

notes_linked <- notes_parsed
if (!is.null(metadata_df)) {
  metadata_names <- names(metadata_df)
  if ("note_file" %in% metadata_names) {
    notes_linked <- notes_parsed %>% left_join(metadata_df, by = "note_file", suffix = c("", "_metadata"))
  } else if ("filename" %in% metadata_names) {
    notes_linked <- notes_parsed %>% left_join(metadata_df, by = c("note_file" = "filename"), suffix = c("", "_metadata"))
  } else if ("file_name" %in% metadata_names) {
    notes_linked <- notes_parsed %>% left_join(metadata_df, by = c("note_file" = "file_name"), suffix = c("", "_metadata"))
  } else {
    warning("Metadata loaded, but no recognized filename column found. No metadata join applied.")
  }
}

pick_first_existing <- function(df, candidates) {
  candidates <- candidates[candidates %in% names(df)]
  if (length(candidates) == 0) return(rep(NA_character_, nrow(df)))
  vals <- df %>% transmute(across(all_of(candidates), as.character))
  coalesce(!!!vals)
}

notes_std <- notes_linked %>%
  mutate(
    mom_id_metadata = pick_first_existing(cur_data_all(), c("deidentified_mom_id", "mom_id", "maternal_id", "mother_id", "patient_mom_id", "mom_deid", "deid_mom_id")),
    infant_id_metadata = pick_first_existing(cur_data_all(), c("deidentified_baby_id", "deidentified_infant_id", "baby_id", "infant_id", "child_id", "newborn_id", "patient_infant_id", "baby_deid", "deid_baby_id")),
    mom_id_clean = coalesce(mom_id_metadata, mom_id_raw, first_numeric_id),
    infant_id_clean = coalesce(infant_id_metadata, infant_id_raw),
    mom_id_std = if_else(!is.na(mom_id_clean) & mom_id_clean != "", paste0("GNV_MOM_", mom_id_clean), NA_character_),
    infant_id_std = if_else(!is.na(infant_id_clean) & infant_id_clean != "", paste0("GNV_INFANT_", infant_id_clean), NA_character_),
    source_notes_dir = notes_dir,
    source_metadata_path = metadata_path,
    standardized_at = as.character(Sys.time())
  ) %>%
  select(
    site, note_set, note_type, troubleshoot_mode, run_label,
    batch_zip, note_file, note_file_path, file_ext, file_size_bytes,
    note_date, note_date_raw,
    mom_id_raw, infant_id_raw, mom_id_metadata, infant_id_metadata,
    mom_id_clean, infant_id_clean, mom_id_std, infant_id_std,
    note_nchar, note_nlines, note_text_preview, note_text,
    source_notes_dir, source_metadata_path, standardized_at,
    everything()
  )

# ============================================================
# 10. Export RDA + CSV
# ============================================================

gnv_delivery_notes_standardized <- notes_std
out_prefix <- paste0("gnv_delivery_notes_standardized_", run_label, "_", date_tag)
out_rda <- file.path(output_dir, paste0(out_prefix, ".rda"))
out_csv_full <- file.path(output_dir, paste0(out_prefix, ".csv"))
out_csv_index <- file.path(output_dir, paste0("gnv_delivery_notes_index_", run_label, "_", date_tag, ".csv"))
out_csv_qc <- file.path(output_dir, paste0("gnv_delivery_notes_qc_", run_label, "_", date_tag, ".csv"))
out_zip_inventory <- file.path(output_dir, paste0("gnv_delivery_notes_zip_inventory_", run_label, "_", date_tag, ".csv"))

save(gnv_delivery_notes_standardized, file = out_rda)
write_csv(gnv_delivery_notes_standardized, out_csv_full)
write_csv(zip_inventory, out_zip_inventory)

gnv_delivery_notes_standardized %>%
  select(site, note_set, note_type, troubleshoot_mode, run_label, batch_zip, note_file, note_file_path, file_ext, file_size_bytes, note_date, mom_id_raw, infant_id_raw, mom_id_metadata, infant_id_metadata, mom_id_std, infant_id_std, note_nchar, note_nlines, note_text_preview, source_metadata_path) %>%
  write_csv(out_csv_index)

qc_summary <- gnv_delivery_notes_standardized %>%
  summarise(
    site = "GNV",
    note_set = "delivery_notes",
    troubleshoot_mode = first(troubleshoot_mode),
    run_label = first(run_label),
    n_zip_batches_total = length(zip_files_all),
    n_zip_batches_processed = length(zip_files),
    n_notes = n(),
    n_batches = n_distinct(batch_zip),
    n_file_ext = n_distinct(file_ext),
    n_missing_text = sum(is.na(note_text) | note_text == ""),
    n_with_note_date = sum(!is.na(note_date)),
    n_with_mom_id_std = sum(!is.na(mom_id_std)),
    n_with_infant_id_std = sum(!is.na(infant_id_std)),
    metadata_loaded = !is.null(metadata_df),
    metadata_path = metadata_path,
    output_rda = out_rda,
    output_csv_full = out_csv_full,
    output_csv_index = out_csv_index
  )
write_csv(qc_summary, out_csv_qc)

message("Saved RDA: ", out_rda)
message("Saved full CSV: ", out_csv_full)
message("Saved CSV index: ", out_csv_index)
message("Saved QC CSV: ", out_csv_qc)
message("Saved zip inventory CSV: ", out_zip_inventory)
message("QC summary:")
print(qc_summary)
message("First 25 standardized rows:")
print(gnv_delivery_notes_standardized %>% select(note_file, file_ext, note_nchar, mom_id_std, infant_id_std, note_text_preview) %>% head(25))
message("==== DONE: STANDARDIZE GNV DELIVERY NOTES ====")
