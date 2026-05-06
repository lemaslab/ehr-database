# ========================================================
# Clinical Notes Metadata Runner
# Purpose: combine 6 note metadata files into one master file
# Sites: GNV, JAX
# Periods: prenatal, delivery, postnatal
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
# Define raw metadata files
# ========================================================
# Replace file names below with the exact raw metadata file names.

metadata_manifest <- tibble::tribble(
  ~site, ~note_period, ~file_path,
  
  "GNV", "prenatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/prenatal_notes_metadata.csv",
  
  "GNV", "delivery",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/delivery_notes_metadata.csv",
  
  "GNV", "postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/postnatal_notes_metadata.csv",
  
  "JAX", "prenatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/prenatal_notes_metadata_Jax.csv",
  
  "JAX", "delivery",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/delivery_notes_metadata_Jax.csv",
  
  "JAX", "postnatal",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/postnatal_notes_metadata_Jax.csv"
)

# ========================================================
# Check files
# ========================================================

metadata_manifest <- metadata_manifest %>%
  mutate(file_exists = file.exists(file_path))

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

read_metadata_file <- function(site, note_period, file_path, file_exists) {
  
  message("Reading: ", site, " | ", note_period, " | ", basename(file_path))
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    clean_names() %>%
    mutate(
      site = site,
      note_period = note_period,
      source_file = basename(file_path),
      source_path = file_path,
      source_n_rows = n()
    ) %>%
    select(
      site,
      note_period,
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
# QC
# ========================================================

cat("\n==== MASTER METADATA QC ====\n")

clinical_notes_metadata %>%
  count(site, note_period, source_file, name = "n_rows") %>%
  arrange(site, note_period, source_file) %>%
  print(n = Inf)

cat("\nTotal rows: ", nrow(clinical_notes_metadata), "\n")
cat("Total columns: ", ncol(clinical_notes_metadata), "\n")

cat("\n==== VARIABLE COMPARISON BY SOURCE FILE ====\n")

variable_qc <- clinical_notes_metadata %>%
  select(site, note_period, source_file) %>%
  distinct()

print(variable_qc, n = Inf)

cat("\nMaster variable names:\n")
print(names(clinical_notes_metadata))

# ========================================================
# Write combined output
# ========================================================

write_dataset(
  clinical_notes_metadata,
  "clinical_notes_metadata_all_sites",
  working_dir,
  "COMBINED"
)

cat("\n==== DONE ====\n")