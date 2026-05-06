# ========================================================
# Clinical Notes Text Standardization Runner
# Purpose: create chunked, one-row-per-note standardized text files
# Input: clinical_notes_metadata_all_sites
# Output root: V:/FACULTY/DJLEMAS/EHR_Data_processed/NOTES
# ========================================================

source("code/functions/utils_load_latest_dataset.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(janitor)
  library(purrr)
  library(stringr)
  library(digest)
})

working_dir <- getwd()
date_tag <- format(Sys.Date(), "%Y%m%d")

# ========================================================
# Output locations
# ========================================================

notes_output_base <- "V:/FACULTY/DJLEMAS/EHR_Data_processed/NOTES"
standardized_output_dir <- file.path(notes_output_base, "standardized")
manifest_output_dir <- file.path(notes_output_base, "manifests")
log_output_dir <- file.path(notes_output_base, "logs")

# ========================================================
# Debug controls
# ========================================================
# In debug mode, read a subset of metadata rows per source_group/source_file
# and write to the local repo debug folder only unless write_network = TRUE.

debug_mode <- TRUE
debug_n_max <- 1000
debug_source_groups <- c("jax_mom_delivery")
# debug_source_groups <- NULL

chunk_size <- 100000

write_network <- FALSE
write_local_debug <- TRUE
local_debug_output_dir <- file.path(working_dir, "data", "debug", "clinical_notes_standardized")

# ========================================================
# Helper functions
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

standardize_note_text <- function(x) {
  x %>%
    as.character() %>%
    str_replace_all("\r\n", "\n") %>%
    str_replace_all("\r", "\n") %>%
    str_replace_all("[ \t]+", " ") %>%
    str_squish()
}

make_note_text_hash <- function(x) {
  case_when(
    !is.na(x) & x != "" ~ map_chr(x, digest, algo = "xxhash64"),
    TRUE ~ NA_character_
  )
}

safe_min_datetime <- function(x) {
  if (all(is.na(x))) return(NA_character_)
  as.character(min(x, na.rm = TRUE))
}

safe_max_datetime <- function(x) {
  if (all(is.na(x))) return(NA_character_)
  as.character(max(x, na.rm = TRUE))
}

make_chunk_id <- function(n_rows, chunk_size) {
  ceiling(seq_len(n_rows) / chunk_size)
}

save_rda_named <- function(df, object_name, file_path) {
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
  assign(object_name, df)
  save(list = object_name, file = file_path)
  rm(list = object_name)
}

# ========================================================
# Source-specific extraction helpers
# ========================================================
# Each source_group may require different logic. This first version handles
# tabular files with one note per row. Individual text-file sources can be
# added as separate source_group branches below.

get_raw_note_id <- function(raw) {
  coalesce_existing(
    raw,
    c(
      "note_id",
      "deid_note_id",
      "deidentified_note_key",
      "deid_linkage_note_id"
    )
  )
}

get_raw_note_text <- function(raw) {
  coalesce_existing(
    raw,
    c(
      "note_text",
      "clinical_note_text",
      "note_body",
      "note_content",
      "report_text",
      "text",
      "note"
    )
  )
}

read_tabular_note_source <- function(meta_source) {
  source_group <- unique(meta_source$source_group)
  source_file <- unique(meta_source$source_file)
  source_path <- unique(meta_source$source_path)
  
  if (length(source_group) != 1 || length(source_file) != 1 || length(source_path) != 1) {
    stop("Expected one source_group/source_file/source_path per source read")
  }
  
  message("Reading source: ", source_group, " | ", source_file)
  
  raw <- read_csv(source_path, show_col_types = FALSE) %>%
    clean_names()
  
  if (debug_mode) {
    raw <- raw %>% slice_head(n = debug_n_max)
  }
  
  raw <- raw %>%
    mutate(
      source_group = source_group,
      source_file = source_file,
      note_id = get_raw_note_id(.),
      note_text = standardize_note_text(get_raw_note_text(.))
    ) %>%
    select(source_group, source_file, note_id, note_text, everything())
  
  meta_source %>%
    mutate(note_id = as.character(note_id)) %>%
    left_join(
      raw %>% select(source_group, source_file, note_id, note_text),
      by = c("source_group", "source_file", "note_id")
    )
}

read_standardized_note_source <- function(meta_source) {
  source_group <- unique(meta_source$source_group)
  
  if (length(source_group) != 1) {
    stop("Expected one source_group per call")
  }
  
  # ======================================================
  # Source-specific branches can be added here.
  # Current default assumes one row per note in a CSV file.
  # ======================================================
  
  if (source_group %in% c(
    "gnv_mom_delivery",
    "gnv_mom_prenatal",
    "gnv_mom_postnatal",
    "gnv_baby_postnatal",
    "jax_mom_prenatal",
    "jax_mom_delivery",
    "jax_mom_postnatal",
    "jax_baby_postnatal"
  )) {
    return(read_tabular_note_source(meta_source))
  }
  
  stop("No reader implemented for source_group: ", source_group)
}

# ========================================================
# Load metadata
# ========================================================

clinical_notes_metadata <- load_latest_dataset(
  "clinical_notes_metadata_all_sites",
  working_dir,
  subdir = "COMBINED"
)

# ========================================================
# Apply debug filters
# ========================================================

if (debug_mode) {
  message("==== DEBUG MODE ENABLED ====")
  
  if (!is.null(debug_source_groups)) {
    clinical_notes_metadata <- clinical_notes_metadata %>%
      filter(source_group %in% debug_source_groups)
  }
  
  clinical_notes_metadata <- clinical_notes_metadata %>%
    group_by(source_group, source_file) %>%
    slice_head(n = debug_n_max) %>%
    ungroup()
  
  message("Debug source groups: ", paste(unique(clinical_notes_metadata$source_group), collapse = ", "))
  message("Debug max rows per source file: ", debug_n_max)
}

if (nrow(clinical_notes_metadata) == 0) {
  stop("No metadata rows available after filters")
}

# ========================================================
# Process one source file at a time
# ========================================================

source_splits <- clinical_notes_metadata %>%
  group_by(site, note_period, participant_role, source_group, source_file, source_path) %>%
  group_split()

manifest_rows <- list()
manifest_i <- 0

for (meta_source in source_splits) {
  
  standardized_source <- read_standardized_note_source(meta_source)
  
  standardized_source <- standardized_source %>%
    mutate(
      note_text_nchar = nchar(note_text),
      note_text_hash = make_note_text_hash(note_text)
    ) %>%
    select(
      any_of(c(
        "part_id",
        "part_id_mom",
        "part_id_infant",
        "note_id",
        "note_created_datetime",
        "site",
        "note_period",
        "participant_role",
        "source_group",
        "source_file",
        "source_path",
        "source_n_rows",
        "note_uid",
        "note_type",
        "note_text",
        "note_text_nchar",
        "note_text_hash"
      )),
      everything()
    )
  
  source_group <- unique(standardized_source$source_group)
  site <- unique(standardized_source$site)
  note_period <- unique(standardized_source$note_period)
  participant_role <- unique(standardized_source$participant_role)
  
  standardized_source <- standardized_source %>%
    mutate(chunk_id = make_chunk_id(n(), chunk_size))
  
  chunk_splits <- standardized_source %>%
    group_by(chunk_id) %>%
    group_split()
  
  for (chunk_df in chunk_splits) {
    chunk_id <- unique(chunk_df$chunk_id)
    chunk_id_chr <- str_pad(chunk_id, width = 3, pad = "0")
    
    chunk_object_name <- "clinical_notes_standardized_chunk"
    chunk_file <- paste0(
      "clinical_notes_standardized_",
      source_group,
      "_part",
      chunk_id_chr,
      "_",
      date_tag,
      ".rda"
    )
    
    relative_chunk_dir <- file.path("standardized", site, note_period)
    network_chunk_dir <- file.path(notes_output_base, relative_chunk_dir)
    local_chunk_dir <- file.path(local_debug_output_dir, relative_chunk_dir)
    
    if (write_network) {
      chunk_path <- file.path(network_chunk_dir, chunk_file)
      save_rda_named(chunk_df, chunk_object_name, chunk_path)
      message("Wrote network chunk: ", chunk_path)
    } else if (debug_mode && write_local_debug) {
      chunk_path <- file.path(local_chunk_dir, chunk_file)
      save_rda_named(chunk_df, chunk_object_name, chunk_path)
      message("Wrote local debug chunk: ", chunk_path)
    } else {
      chunk_path <- NA_character_
      message("Skipping chunk write for: ", source_group, " part", chunk_id_chr)
    }
    
    manifest_i <- manifest_i + 1
    manifest_rows[[manifest_i]] <- tibble(
      site = site,
      note_period = note_period,
      participant_role = participant_role,
      source_group = source_group,
      source_file = unique(chunk_df$source_file),
      source_path = unique(chunk_df$source_path),
      chunk_id = chunk_id,
      chunk_file = chunk_file,
      chunk_path = chunk_path,
      n_rows = nrow(chunk_df),
      n_missing_note_text = sum(is.na(chunk_df$note_text) | chunk_df$note_text == ""),
      n_missing_note_uid = sum(is.na(chunk_df$note_uid) | chunk_df$note_uid == ""),
      n_duplicated_note_uid = sum(duplicated(chunk_df$note_uid[!is.na(chunk_df$note_uid) & chunk_df$note_uid != ""])),
      min_note_created_datetime = safe_min_datetime(chunk_df$note_created_datetime),
      max_note_created_datetime = safe_max_datetime(chunk_df$note_created_datetime),
      created_at = as.character(Sys.time())
    )
  }
}

# ========================================================
# Save manifest
# ========================================================

clinical_notes_standardized_manifest <- bind_rows(manifest_rows)

cat("\n==== STANDARDIZED NOTES MANIFEST QC ====\n")
print(clinical_notes_standardized_manifest, n = Inf)

cat("\n==== SUMMARY BY SOURCE GROUP ====\n")
clinical_notes_standardized_manifest %>%
  group_by(site, note_period, participant_role, source_group) %>%
  summarise(
    n_chunks = n(),
    n_rows = sum(n_rows),
    n_missing_note_text = sum(n_missing_note_text),
    n_missing_note_uid = sum(n_missing_note_uid),
    n_duplicated_note_uid = sum(n_duplicated_note_uid),
    .groups = "drop"
  ) %>%
  arrange(site, note_period, participant_role, source_group) %>%
  print(n = Inf)

manifest_file_base <- paste0("clinical_notes_standardized_manifest_", date_tag)

if (write_network) {
  dir.create(manifest_output_dir, recursive = TRUE, showWarnings = FALSE)
  save_rda_named(
    clinical_notes_standardized_manifest,
    "clinical_notes_standardized_manifest",
    file.path(manifest_output_dir, paste0(manifest_file_base, ".rda"))
  )
  write_csv(
    clinical_notes_standardized_manifest,
    file.path(manifest_output_dir, paste0(manifest_file_base, ".csv")),
    na = ""
  )
  message("Wrote network manifest: ", manifest_output_dir)
}

if (debug_mode && write_local_debug) {
  dir.create(local_debug_output_dir, recursive = TRUE, showWarnings = FALSE)
  save_rda_named(
    clinical_notes_standardized_manifest,
    "clinical_notes_standardized_manifest",
    file.path(local_debug_output_dir, paste0(manifest_file_base, ".rda"))
  )
  write_csv(
    clinical_notes_standardized_manifest,
    file.path(local_debug_output_dir, paste0(manifest_file_base, ".csv")),
    na = ""
  )
  message("Wrote local debug manifest: ", local_debug_output_dir)
}

cat("\n==== DONE ====\n")
