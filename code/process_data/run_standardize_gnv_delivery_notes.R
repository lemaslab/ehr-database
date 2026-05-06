# ============================================================
# Stand-alone script: Standardize GNV Delivery Notes
# ============================================================

library(dplyr)
library(readr)
library(stringr)
library(purrr)
library(tibble)
library(fs)

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

if (is.null(working_dir)) {
  stop("Could not locate repo root.")
}

message("Using working_dir: ", working_dir)

# ============================================================
# 02. Paths
# ============================================================

notes_dir <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_10_2021/delivery_notes"

if (!dir.exists(notes_dir)) {
  stop("notes_dir does not exist: ", notes_dir)
}

date_tag <- format(Sys.Date(), "%Y%m%d")

output_dir <- file.path(working_dir, "data", "processed", "notes")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

extract_dir <- file.path(output_dir, "gnv_delivery_notes_unzipped")
dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)

# ============================================================
# 03. Logging
# ============================================================

log_dir <- file.path(working_dir, "logs")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

log_file <- file.path(
  log_dir,
  paste0("standardize_gnv_delivery_notes_", date_tag, ".log")
)

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
sink(log_con, type = "message")

on.exit({
  sink(type = "message")
  sink()
  close(log_con)
}, add = TRUE)

message("==== STANDARDIZE GNV DELIVERY NOTES ====")
message("Run time: ", Sys.time())
message("notes_dir: ", notes_dir)
message("output_dir: ", output_dir)
message("extract_dir: ", extract_dir)
message("log_file: ", log_file)

# ============================================================
# 04. Locate zip batches
# ============================================================

zip_files <- dir_ls(
  notes_dir,
  regexp = "\\.zip$",
  recurse = FALSE,
  type = "file"
)

message("Zip batches found: ", length(zip_files))
print(zip_files)

if (length(zip_files) == 0) {
  stop("No .zip files found in notes_dir: ", notes_dir)
}

if (length(zip_files) != 6) {
  warning("Expected 6 zip batches, but found ", length(zip_files), ". Continuing.")
}

# ============================================================
# 05. Unzip batches
# ============================================================

for (zip_file in zip_files) {
  
  batch_name <- path_ext_remove(path_file(zip_file))
  batch_extract_dir <- file.path(extract_dir, batch_name)
  
  dir.create(batch_extract_dir, recursive = TRUE, showWarnings = FALSE)
  
  message("Unzipping: ", path_file(zip_file))
  
  unzip(
    zipfile = zip_file,
    exdir = batch_extract_dir
  )
}

# ============================================================
# 06. Inventory extracted note files
# ============================================================

note_files <- dir_ls(
  extract_dir,
  recurse = TRUE,
  type = "file"
)

if (length(note_files) == 0) {
  stop("No files found after unzipping to: ", extract_dir)
}

note_inventory <- tibble(
  site = "GNV",
  note_set = "delivery_notes",
  note_type = "delivery",
  note_file_path = as.character(note_files),
  note_file = path_file(note_files),
  batch_zip = path_file(path_dir(note_files)),
  file_ext = str_to_lower(path_ext(note_files)),
  file_size_bytes = as.numeric(file_size(note_files))
)

message("Extracted note files: ", nrow(note_inventory))
print(note_inventory %>% count(batch_zip, file_ext, name = "n_files"))

# ============================================================
# 07. Read note text
# ============================================================

read_note_safely <- function(path) {
  tryCatch(
    {
      paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
    },
    error = function(e) {
      warning("Could not read file: ", path, " | ", conditionMessage(e))
      NA_character_
    }
  )
}

notes_raw <- note_inventory %>%
  mutate(
    note_text = map_chr(note_file_path, read_note_safely),
    note_nchar = nchar(note_text),
    note_nlines = if_else(
      is.na(note_text),
      NA_integer_,
      str_count(note_text, "\n") + 1L
    )
  )

# ============================================================
# 08. Find optional metadata file
# ============================================================
# This searches for a metadata/index file in the same delivery_notes folder.
# If multiple files match, no join is done until metadata_path is set manually.

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
  warning("Multiple candidate metadata files found. Set metadata_path manually.")
} else {
  warning("No candidate metadata file found. Continuing without metadata join.")
}

read_metadata_safely <- function(path) {
  
  if (is.na(path) || !file.exists(path)) {
    return(NULL)
  }
  
  ext <- str_to_lower(path_ext(path))
  
  if (ext == "csv") {
    return(read_csv(path, show_col_types = FALSE))
  }
  
  if (ext == "rds") {
    return(readRDS(path))
  }
  
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
# 09. Parse filename details
# ============================================================

notes_parsed <- notes_raw %>%
  mutate(
    note_file_stem = path_ext_remove(note_file),
    
    date_yyyymmdd = str_extract(
      note_file,
      "(?<!\\d)(19|20)\\d{6}(?!\\d)"
    ),
    
    date_yyyy_mm_dd = str_extract(
      note_file,
      "(?<!\\d)(19|20)\\d{2}[-_]\\d{2}[-_]\\d{2}(?!\\d)"
    ),
    
    note_date_raw = coalesce(date_yyyy_mm_dd, date_yyyymmdd),
    
    note_date = case_when(
      !is.na(date_yyyy_mm_dd) ~ as.Date(str_replace_all(date_yyyy_mm_dd, "_", "-")),
      !is.na(date_yyyymmdd) ~ as.Date(date_yyyymmdd, format = "%Y%m%d"),
      TRUE ~ as.Date(NA)
    ),
    
    mom_id_raw = str_extract(
      note_file,
      "(?i)(mom|mother|maternal)[_-]?[A-Za-z0-9]+"
    ),
    
    infant_id_raw = str_extract(
      note_file,
      "(?i)(baby|infant|child|newborn)[_-]?[A-Za-z0-9]+"
    ),
    
    first_numeric_id = str_extract(
      note_file,
      "(?<!\\d)\\d{4,}(?!\\d)"
    )
  )

# ============================================================
# 10. Optional metadata join
# ============================================================

notes_linked <- notes_parsed

if (!is.null(metadata_df)) {
  
  metadata_names <- names(metadata_df)
  
  if ("note_file" %in% metadata_names) {
    
    notes_linked <- notes_parsed %>%
      left_join(metadata_df, by = "note_file", suffix = c("", "_metadata"))
    
    message("Metadata joined by note_file.")
    
  } else if ("filename" %in% metadata_names) {
    
    notes_linked <- notes_parsed %>%
      left_join(metadata_df, by = c("note_file" = "filename"), suffix = c("", "_metadata"))
    
    message("Metadata joined by note_file = filename.")
    
  } else if ("file_name" %in% metadata_names) {
    
    notes_linked <- notes_parsed %>%
      left_join(metadata_df, by = c("note_file" = "file_name"), suffix = c("", "_metadata"))
    
    message("Metadata joined by note_file = file_name.")
    
  } else {
    
    warning("Metadata loaded, but no recognized filename column found. No join applied.")
  }
}

# ============================================================
# 11. Helper: pick first existing column
# ============================================================

pick_first_existing <- function(df, candidates) {
  
  candidates <- candidates[candidates %in% names(df)]
  
  if (length(candidates) == 0) {
    return(rep(NA_character_, nrow(df)))
  }
  
  vals <- df %>%
    transmute(across(all_of(candidates), as.character))
  
  coalesce(!!!vals)
}

# ============================================================
# 12. Standardize output
# ============================================================

notes_std <- notes_linked %>%
  mutate(
    mom_id_metadata = pick_first_existing(
      cur_data_all(),
      c(
        "deidentified_mom_id",
        "mom_id",
        "maternal_id",
        "mother_id",
        "patient_mom_id",
        "mom_deid",
        "deid_mom_id"
      )
    ),
    
    infant_id_metadata = pick_first_existing(
      cur_data_all(),
      c(
        "deidentified_baby_id",
        "deidentified_infant_id",
        "baby_id",
        "infant_id",
        "child_id",
        "newborn_id",
        "patient_infant_id",
        "baby_deid",
        "deid_baby_id"
      )
    ),
    
    mom_id_clean = coalesce(
      mom_id_metadata,
      mom_id_raw,
      first_numeric_id
    ),
    
    infant_id_clean = coalesce(
      infant_id_metadata,
      infant_id_raw
    ),
    
    mom_id_std = if_else(
      !is.na(mom_id_clean) & mom_id_clean != "",
      paste0("GNV_MOM_", mom_id_clean),
      NA_character_
    ),
    
    infant_id_std = if_else(
      !is.na(infant_id_clean) & infant_id_clean != "",
      paste0("GNV_INFANT_", infant_id_clean),
      NA_character_
    ),
    
    source_notes_dir = notes_dir,
    source_metadata_path = metadata_path,
    standardized_at = as.character(Sys.time())
  ) %>%
  select(
    site,
    note_set,
    note_type,
    batch_zip,
    note_file,
    note_file_path,
    file_ext,
    file_size_bytes,
    note_date,
    note_date_raw,
    mom_id_raw,
    infant_id_raw,
    mom_id_metadata,
    infant_id_metadata,
    mom_id_clean,
    infant_id_clean,
    mom_id_std,
    infant_id_std,
    note_nchar,
    note_nlines,
    note_text,
    source_notes_dir,
    source_metadata_path,
    standardized_at,
    everything()
  )

# ============================================================
# 13. Export RDA + CSV
# ============================================================

gnv_delivery_notes_standardized <- notes_std

out_rda <- file.path(
  output_dir,
  paste0("gnv_delivery_notes_standardized_", date_tag, ".rda")
)

out_csv_full <- file.path(
  output_dir,
  paste0("gnv_delivery_notes_standardized_", date_tag, ".csv")
)

out_csv_index <- file.path(
  output_dir,
  paste0("gnv_delivery_notes_index_", date_tag, ".csv")
)

out_csv_qc <- file.path(
  output_dir,
  paste0("gnv_delivery_notes_qc_", date_tag, ".csv")
)

save(gnv_delivery_notes_standardized, file = out_rda)
write_csv(gnv_delivery_notes_standardized, out_csv_full)

gnv_delivery_notes_standardized %>%
  select(
    site,
    note_set,
    note_type,
    batch_zip,
    note_file,
    note_file_path,
    file_ext,
    file_size_bytes,
    note_date,
    mom_id_raw,
    infant_id_raw,
    mom_id_metadata,
    infant_id_metadata,
    mom_id_std,
    infant_id_std,
    note_nchar,
    note_nlines,
    source_metadata_path
  ) %>%
  write_csv(out_csv_index)

qc_summary <- gnv_delivery_notes_standardized %>%
  summarise(
    site = "GNV",
    note_set = "delivery_notes",
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

# ============================================================
# 14. QC printout
# ============================================================

message("QC summary:")
print(qc_summary)

message("Inventory by batch and extension:")
print(
  gnv_delivery_notes_standardized %>%
    count(batch_zip, file_ext, name = "n_files")
)

message("First 25 note filenames:")
print(head(gnv_delivery_notes_standardized$note_file, 25))

message("==== DONE: STANDARDIZE GNV DELIVERY NOTES ====")
