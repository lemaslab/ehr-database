# ========================================================
# Clinical Notes Metadata Runner
# Purpose: combine clinical note metadata files into one master file
# Sites: GNV, JAX
# Periods: prenatal, delivery, postnatal
#
# Debug options:
# - debug_mode = TRUE reads fewer rows/files for faster troubleshooting
# - write_outputs = FALSE skips slow shared-drive write_dataset()
# - write_debug_local = TRUE writes a small local RDA to data/debug
# ========================================================

source("code/functions/utils_write_dataset.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(janitor)
  library(purrr)
  library(stringr)
})

working_dir <- getwd()

# ========================================================
# Debug controls
# ========================================================
# Set debug_mode <- TRUE when troubleshooting computed variables.
# Set debug_mode <- FALSE and write_outputs <- TRUE for production.

debug_mode <- FALSE

# Number of rows to read per source file in debug mode.
debug_n_max <- 1000

# Limit debugging to one or more source groups.
# Use NULL to include all groups.
debug_source_groups <- NULL

# Examples:
# debug_source_groups <- c("gnv_mom_delivery")
# debug_source_groups <- c("gnv_mom_prenatal")
# debug_source_groups <- c("gnv_mom_postnatal")
# debug_source_groups <- c("gnv_baby_postnatal")
# debug_source_groups <- c("jax_mom_prenatal")
# debug_source_groups <- c("jax_mom_delivery")
# debug_source_groups <- c("jax_mom_postnatal")
# debug_source_groups <- c("jax_baby_postnatal")
# debug_source_groups <- NULL

# Skip slow shared-drive write while debugging.
write_outputs <- TRUE

# Optional small local debug file.
write_debug_local <- FALSE
debug_output_dir <- file.path(working_dir, "data", "debug")

# ========================================================
# Helper: pull first existing variable from possible names
# ========================================================

coalesce_existing <- function(df, vars) {
  
  existing_vars <- intersect(vars, names(df))
  
  if (length(existing_vars) == 0) {
    return(rep(NA_character_, nrow(df)))
  }
  
  df %>%
    mutate(across(all_of(existing_vars), as.character)) %>%
    transmute(value = coalesce(!!!syms(existing_vars))) %>%
    pull(value)
}

# ========================================================
# Helper: clean source note ID before compact standardization
# ========================================================

clean_source_note_id <- function(x) {
  x %>%
    as.character() %>%
    str_squish() %>%
    str_remove(regex("^NOTE_", ignore_case = TRUE))
}

# ========================================================
# Define raw metadata files
# ========================================================

metadata_manifest <- tibble::tribble(
  ~site, ~note_period, ~participant_role, ~source_group, ~file_path,
  
  # -------------------------------
  # GNV mom notes
  # -------------------------------
  "GNV", "delivery", "mom", "gnv_mom_delivery",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_10_2021/delivery_notes/mom_notes_at_delivery.csv",
  
  "GNV", "prenatal", "mom", "gnv_mom_prenatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_10_2021/prenatal_notes/mom_notes_prenatal_visit.csv",
  
  # -------------------------------
  # GNV baby postnatal notes
  # -------------------------------
  "GNV", "postnatal", "baby", "gnv_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/baby_subjects_clinical_notes_details/subjects_clinical_notes_details_1.csv",
  
  "GNV", "postnatal", "baby", "gnv_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/baby_subjects_clinical_notes_details/subjects_clinical_notes_details_2.csv",
  
  "GNV", "postnatal", "baby", "gnv_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/baby_subjects_clinical_notes_details/subjects_clinical_notes_details_3.csv",
  
  "GNV", "postnatal", "baby", "gnv_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/baby_subjects_clinical_notes_details/subjects_clinical_notes_details_4.csv",
  
  "GNV", "postnatal", "baby", "gnv_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/baby_subjects_clinical_notes_details/subjects_clinical_notes_details_5.csv",
  
  "GNV", "postnatal", "baby", "gnv_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/baby_subjects_clinical_notes_details/subjects_clinical_notes_details_6.csv",
  
  "GNV", "postnatal", "baby", "gnv_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/baby_subjects_clinical_notes_details/subjects_clinical_notes_details_7.csv",
  
  "GNV", "postnatal", "baby", "gnv_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/baby_subjects_clinical_notes_details/subjects_clinical_notes_details_8.csv",
  
  # -------------------------------
  # GNV mom postnatal notes
  # -------------------------------
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_1.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_2.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_3.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_4.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_5.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_6.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_7.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_8.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_9.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_10.csv",
  
  "GNV", "postnatal", "mom", "gnv_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_11_2021/mom_subjects_clinical_notes_details/subjects_clinical_notes_details_11.csv",
  
  # -------------------------------
  # JAX notes
  # -------------------------------
  "JAX", "prenatal", "mom", "jax_mom_prenatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_05_2025/data_release_05_2025/note_mom_prenatal/mom_note_metadata_at_prenatal_Jax.csv",
  
  "JAX", "delivery", "mom", "jax_mom_delivery",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_05_2025/data_release_05_2025/note_mom_delivery/mom_note_metadata_at_delivery_Jax.csv",
  
  "JAX", "postnatal", "baby", "jax_baby_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_05_2025/data_release_05_2025/note_baby_postnatal_two_years/postnatal_baby_metadata_Jax2.csv",
  
  "JAX", "postnatal", "mom", "jax_mom_postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_05_2025/data_release_05_2025/note_mom_postnatal_two_years/postnatal_mom_metadata_Jax_update.csv"
)

# ========================================================
# Apply debug filters
# ========================================================

if (debug_mode) {
  
  message("==== DEBUG MODE ENABLED ====")
  
  if (!is.null(debug_source_groups)) {
    metadata_manifest <- metadata_manifest %>%
      filter(source_group %in% debug_source_groups)
  }
  
  message("Debug source groups: ", paste(unique(metadata_manifest$source_group), collapse = ", "))
  message("Debug max rows per file: ", debug_n_max)
  message("Write full outputs: ", write_outputs)
  message("Write local debug output: ", write_debug_local)
}

# ========================================================
# Check files
# ========================================================

metadata_manifest <- metadata_manifest %>%
  mutate(file_exists = file.exists(file_path))

if (nrow(metadata_manifest) == 0) {
  stop("metadata_manifest has zero rows. Check debug_source_groups.")
}

if (any(!metadata_manifest$file_exists)) {
  cat("\n==== MISSING FILES ====\n")
  print(metadata_manifest %>% filter(!file_exists), n = Inf)
  stop("One or more clinical note metadata files are missing.")
}

cat("\n==== FILE MANIFEST ====\n")
print(metadata_manifest, n = Inf)

# ========================================================
# Read one metadata file
# ========================================================

read_metadata_file <- function(site, note_period, participant_role,
                               source_group, file_path, file_exists) {
  
  message(
    "Reading: ", site, " | ", note_period, " | ", participant_role,
    " | ", source_group, " | ", basename(file_path)
  )
  
  n_max_arg <- if (debug_mode) debug_n_max else Inf
  
  df <- read_csv(
    file_path,
    show_col_types = FALSE,
    n_max = n_max_arg
  ) %>%
    clean_names() %>%
    mutate(
      site = site,
      note_period = note_period,
      participant_role = participant_role,
      source_group = source_group,
      source_file = basename(file_path),
      source_path = file_path,
      source_n_rows = n()
    ) %>%
    select(
      site,
      note_period,
      participant_role,
      source_group,
      source_file,
      source_path,
      source_n_rows,
      everything()
    )
  
  message("Rows read: ", nrow(df))
  message("Columns read: ", ncol(df))
  
  return(df)
}

# ========================================================
# Combine files
# ========================================================

clinical_notes_metadata <- pmap_dfr(
  metadata_manifest,
  read_metadata_file
)

# ========================================================
# Standardize core participant and note ID variables
# ========================================================

clinical_notes_metadata <- clinical_notes_metadata %>%
  mutate(
    site_prefix = case_when(
      site == "GNV" ~ "AC",
      site == "JAX" ~ "DC",
      TRUE ~ NA_character_
    ),
    
    role_code = case_when(
      participant_role == "mom" ~ "M",
      participant_role %in% c("baby", "infant") ~ "B",
      TRUE ~ "U"
    ),
    
    period_code = case_when(
      note_period == "prenatal" ~ "PN",
      note_period == "delivery" ~ "DL",
      note_period == "postnatal" ~ "PP",
      TRUE ~ "UNK"
    ),
    
    part_id_mom_raw = coalesce_existing(
      .,
      c(
        "part_id_mom",
        "deidentified_mom_id",
        "deidentified_mother_id",
        "deid_mom_id",
        "mom_id",
        "mother_id",
        "maternal_id"
      )
    ),
    
    part_id_infant_raw = coalesce_existing(
      .,
      c(
        "part_id_infant",
        "deidentified_infant_id",
        "deidentified_baby_id",
        "deid_baby_id",
        "deid_infant_id",
        "baby_id",
        "infant_id",
        "child_id"
      )
    ),
    
    # Raw note ID is preserved for linking back to raw note files.
    # note_id_source is cleaned for compact standardized IDs.
    note_id_raw = coalesce_existing(
      .,
      c(
        "note_id",
        "deid_note_id",
        "deidentified_note_key",
        "deid_linkage_note_id"
      )
    ),
    note_id_source = clean_source_note_id(note_id_raw),
    
    part_id_mom = case_when(
      !is.na(part_id_mom_raw) & part_id_mom_raw != "" ~
        paste0(site_prefix, "-mom-", part_id_mom_raw),
      TRUE ~ NA_character_
    ),
    
    part_id_infant = case_when(
      !is.na(part_id_infant_raw) & part_id_infant_raw != "" ~
        paste0(site_prefix, "-infant-", part_id_infant_raw),
      TRUE ~ NA_character_
    ),
    
    # Each note should belong to one participant only.
    # part_id is the single note-level participant ID.
    part_id = coalesce(part_id_mom, part_id_infant),
    
    # Compact standardized note ID.
    # Example: AC_M_DL_1150, DC_B_PP_1150.
    note_uid = case_when(
      !is.na(note_id_source) & note_id_source != "" ~
        paste(site_prefix, role_code, period_code, note_id_source, sep = "_"),
      TRUE ~ NA_character_
    ),
    note_id = note_uid,
    
    # Globally unique note key for downstream note-level analysis.
    # This protects against repeated note IDs across source files.
    note_uid_global = paste(site, source_group, source_file, note_uid, sep = "::")
  ) %>%
  select(
    any_of(c(
      "part_id",
      "part_id_mom",
      "part_id_infant",
      "note_id",
      "note_uid",
      "note_uid_global",
      "note_id_raw",
      "note_id_source",
      "note_created_datetime",
      "site",
      "note_period",
      "participant_role",
      "source_group",
      "source_file",
      "source_path",
      "source_n_rows",
      "note_type"
    )),
    everything()
  ) %>%
  select(
    -site_prefix,
    -role_code,
    -period_code,
    -part_id_mom_raw,
    -part_id_infant_raw,
    -any_of(c(
      "deidentified_mom_id",
      "deidentified_mother_id",
      "deid_mom_id",
      "mom_id",
      "mother_id",
      "maternal_id",
      "deidentified_infant_id",
      "deidentified_baby_id",
      "deid_baby_id",
      "deid_infant_id",
      "baby_id",
      "infant_id",
      "child_id",
      "deid_note_id",
      "deidentified_note_key",
      "deid_linkage_note_id"
    ))
  )

# ========================================================
# PLACEHOLDER: troubleshoot additional computed variables here
# ========================================================
# Add/test computed variables here while debug_mode = TRUE.
# This avoids repeatedly writing the large master file to the shared drive.
#
# Example:
#
# clinical_notes_metadata <- clinical_notes_metadata %>%
#   mutate(
#     note_text_nchar = if ("note_text" %in% names(.)) {
#       nchar(note_text)
#     } else {
#       NA_integer_
#     }
#   )

# ========================================================
# QC
# ========================================================

cat("\n==== MASTER METADATA QC ====\n")

clinical_notes_metadata %>%
  count(site, note_period, participant_role, source_group, source_file, name = "n_rows") %>%
  arrange(site, note_period, participant_role, source_group, source_file) %>%
  print(n = Inf)

cat("\nTotal rows: ", nrow(clinical_notes_metadata), "\n")
cat("Total columns: ", ncol(clinical_notes_metadata), "\n")

cat("\n==== SOURCE GROUP QC ====\n")

clinical_notes_metadata %>%
  count(site, note_period, participant_role, source_group, name = "n_rows") %>%
  arrange(site, note_period, participant_role, source_group) %>%
  print(n = Inf)

cat("\n==== ID MISSINGNESS QC ====\n")

clinical_notes_metadata %>%
  summarise(
    n_rows = n(),
    missing_part_id = sum(is.na(part_id) | part_id == ""),
    missing_part_id_mom = sum(is.na(part_id_mom) | part_id_mom == ""),
    missing_part_id_infant = sum(is.na(part_id_infant) | part_id_infant == ""),
    both_mom_and_infant_ids = sum(
      !is.na(part_id_mom) & part_id_mom != "" &
        !is.na(part_id_infant) & part_id_infant != ""
    )
  ) %>%
  print()

cat("\n==== ID MISSINGNESS BY GROUP ====\n")

clinical_notes_metadata %>%
  group_by(site, note_period, participant_role, source_group) %>%
  summarise(
    n_rows = n(),
    missing_part_id = sum(is.na(part_id) | part_id == ""),
    missing_part_id_mom = sum(is.na(part_id_mom) | part_id_mom == ""),
    missing_part_id_infant = sum(is.na(part_id_infant) | part_id_infant == ""),
    both_mom_and_infant_ids = sum(
      !is.na(part_id_mom) & part_id_mom != "" &
        !is.na(part_id_infant) & part_id_infant != ""
    ),
    .groups = "drop"
  ) %>%
  arrange(site, note_period, participant_role, source_group) %>%
  print(n = Inf)

cat("\n==== NOTE ID MISSINGNESS QC ====\n")

clinical_notes_metadata %>%
  summarise(
    n_rows = n(),
    missing_note_id = sum(is.na(note_id) | note_id == ""),
    missing_note_uid = sum(is.na(note_uid) | note_uid == ""),
    missing_note_uid_global = sum(is.na(note_uid_global) | note_uid_global == "" | str_detect(note_uid_global, "::NA$")),
    missing_note_id_raw = sum(is.na(note_id_raw) | note_id_raw == ""),
    missing_note_id_source = sum(is.na(note_id_source) | note_id_source == ""),
    duplicated_note_id = sum(duplicated(note_id[!is.na(note_id) & note_id != ""])),
    duplicated_note_uid = sum(duplicated(note_uid[!is.na(note_uid) & note_uid != ""])),
    duplicated_note_uid_global = sum(duplicated(note_uid_global[!is.na(note_uid_global) & note_uid_global != "" & !str_detect(note_uid_global, "::NA$")]))
  ) %>%
  print()

cat("\n==== NOTE ID MISSINGNESS BY GROUP ====\n")

clinical_notes_metadata %>%
  group_by(site, note_period, participant_role, source_group) %>%
  summarise(
    n_rows = n(),
    missing_note_id = sum(is.na(note_id) | note_id == ""),
    missing_note_uid = sum(is.na(note_uid) | note_uid == ""),
    missing_note_uid_global = sum(is.na(note_uid_global) | note_uid_global == "" | str_detect(note_uid_global, "::NA$")),
    missing_note_id_raw = sum(is.na(note_id_raw) | note_id_raw == ""),
    missing_note_id_source = sum(is.na(note_id_source) | note_id_source == ""),
    duplicated_note_id = sum(duplicated(note_id[!is.na(note_id) & note_id != ""])),
    duplicated_note_uid = sum(duplicated(note_uid[!is.na(note_uid) & note_uid != ""])),
    duplicated_note_uid_global = sum(duplicated(note_uid_global[!is.na(note_uid_global) & note_uid_global != "" & !str_detect(note_uid_global, "::NA$")])),
    .groups = "drop"
  ) %>%
  arrange(site, note_period, participant_role, source_group) %>%
  print(n = Inf)

# ========================================================
# NOTE ID UNIQUENESS QC
# ========================================================

cat("\n==== NOTE UID UNIQUENESS QC: WITHIN SOURCE FILE ====\n")

note_uid_within_file_qc <- clinical_notes_metadata %>%
  filter(!is.na(note_uid), note_uid != "") %>%
  count(site, source_group, source_file, note_uid, name = "n_rows_per_note_uid") %>%
  filter(n_rows_per_note_uid > 1) %>%
  arrange(site, source_group, source_file, desc(n_rows_per_note_uid))

if (nrow(note_uid_within_file_qc) == 0) {
  cat("No duplicated compact note_uid values within any source file.\n")
} else {
  print(note_uid_within_file_qc, n = Inf)
}

cat("\n==== NOTE UID UNIQUENESS QC: ACROSS SOURCE FILES ====\n")

note_uid_across_file_qc <- clinical_notes_metadata %>%
  filter(!is.na(note_uid), note_uid != "") %>%
  distinct(site, source_group, source_file, note_uid) %>%
  count(note_uid, name = "n_source_files") %>%
  filter(n_source_files > 1) %>%
  arrange(desc(n_source_files), note_uid)

if (nrow(note_uid_across_file_qc) == 0) {
  cat("No compact note_uid values appear in more than one source file.\n")
} else {
  print(note_uid_across_file_qc, n = Inf)
}

cat("\n==== NOTE UID GLOBAL UNIQUENESS QC ====\n")

note_uid_global_qc <- clinical_notes_metadata %>%
  filter(!is.na(note_uid_global), note_uid_global != "", !str_detect(note_uid_global, "::NA$")) %>%
  count(note_uid_global, name = "n_rows_per_note_uid_global") %>%
  filter(n_rows_per_note_uid_global > 1) %>%
  arrange(desc(n_rows_per_note_uid_global), note_uid_global)

if (nrow(note_uid_global_qc) == 0) {
  cat("No duplicated note_uid_global values in the combined metadata.\n")
} else {
  print(note_uid_global_qc, n = Inf)
}

cat("\n==== NOTE UID SUMMARY BY SOURCE FILE ====\n")

clinical_notes_metadata %>%
  group_by(site, note_period, participant_role, source_group, source_file) %>%
  summarise(
    n_rows = n(),
    n_missing_note_uid = sum(is.na(note_uid) | note_uid == ""),
    n_unique_note_uid = n_distinct(note_uid[!is.na(note_uid) & note_uid != ""]),
    n_duplicated_note_uid_rows = n_rows - n_missing_note_uid - n_unique_note_uid,
    .groups = "drop"
  ) %>%
  arrange(site, note_period, participant_role, source_group, source_file) %>%
  print(n = Inf)

cat("\n==== NOTE UID FORMAT QC ====\n")

clinical_notes_metadata %>%
  mutate(
    note_uid_prefix = if_else(!is.na(note_uid), str_extract(note_uid, "^[^_]+_[^_]+_[^_]+"), NA_character_),
    raw_starts_with_note = if_else(!is.na(note_id_raw), str_detect(note_id_raw, regex("^NOTE_", ignore_case = TRUE)), NA),
    source_starts_with_note = if_else(!is.na(note_id_source), str_detect(note_id_source, regex("^NOTE_", ignore_case = TRUE)), NA)
  ) %>%
  count(site, participant_role, note_period, source_group, note_uid_prefix, raw_starts_with_note, source_starts_with_note, name = "n_rows") %>%
  arrange(site, participant_role, note_period, source_group, note_uid_prefix, raw_starts_with_note, source_starts_with_note) %>%
  print(n = Inf)

cat("\n==== ID PREFIX QC ====\n")

clinical_notes_metadata %>%
  mutate(
    part_prefix = if_else(!is.na(part_id), str_extract(part_id, "^[^-]+-[^-]+"), NA_character_),
    mom_prefix = if_else(!is.na(part_id_mom), str_extract(part_id_mom, "^[^-]+-mom"), NA_character_),
    infant_prefix = if_else(!is.na(part_id_infant), str_extract(part_id_infant, "^[^-]+-infant"), NA_character_)
  ) %>%
  count(site, participant_role, source_group, part_prefix, mom_prefix, infant_prefix, name = "n_rows") %>%
  arrange(site, participant_role, source_group, part_prefix, mom_prefix, infant_prefix) %>%
  print(n = Inf)

cat("\n==== MASTER VARIABLE NAMES ====\n")
print(names(clinical_notes_metadata))

# ========================================================
# Optional: inspect selected variables during debugging
# ========================================================

if (debug_mode) {
  
  cat("\n==== DEBUG PREVIEW ====\n")
  
  print(
    clinical_notes_metadata %>%
      select(
        any_of(c(
          "part_id",
          "part_id_mom",
          "part_id_infant",
          "note_id",
          "note_uid",
          "note_uid_global",
          "note_id_raw",
          "note_id_source",
          "note_created_datetime",
          "site",
          "note_period",
          "participant_role",
          "source_group",
          "source_file",
          "source_path",
          "source_n_rows",
          "note_type"
        )),
        everything()
      ) %>%
      head(10)
  )
}

# ========================================================
# Write combined output
# ========================================================

if (write_outputs) {
  
  write_dataset(
    clinical_notes_metadata,
    "clinical_notes_metadata_all_sites",
    working_dir,
    "COMBINED"
  )
  
} else {
  
  message("Skipping write_dataset() because write_outputs = FALSE")
}

# ========================================================
# Optional local debug output as RDA
# ========================================================

if (debug_mode && write_debug_local) {
  
  dir.create(debug_output_dir, recursive = TRUE, showWarnings = FALSE)
  
  debug_file <- file.path(
    debug_output_dir,
    paste0("clinical_notes_metadata_debug_", format(Sys.Date(), "%Y%m%d"), ".rda")
  )
  
  save(clinical_notes_metadata, file = debug_file)
  
  message("Wrote local debug RDA file: ", debug_file)
}

cat("\n==== DONE ====\n")