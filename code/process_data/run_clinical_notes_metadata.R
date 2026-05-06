# ========================================================
# Clinical Notes Metadata Runner
# Purpose: combine clinical note metadata files into one master file
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

read_metadata_file <- function(site, note_period, participant_role,
                               source_group, file_path, file_exists) {

  message("Reading: ", site, " | ", note_period, " | ", participant_role,
          " | ", basename(file_path))

  df <- read_csv(file_path, show_col_types = FALSE) %>%
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

cat("\n==== MASTER VARIABLE NAMES ====\n")
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
