process_maternal_icd_codes_v1 <- function(site, working_dir, mom_baby_link_df) {
  
  message("=== PROCESSING MATERNAL ICD: ", site, " ===")
  
  library(dplyr)
  library(readr)
  library(lubridate)
  
  # ===============================
  # Define file paths by site
  # ===============================
  base_root <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data"
  
  if (site == "GNV") {
    
    files <- list(
      icd9        = file.path(base_root, "2021/dataset_10_2021/mom_diagnosis_ICD9.csv"),
      icd10       = file.path(base_root, "2021/dataset_10_2021/mom_diagnosis_ICD10.csv"),
      comorb_icd9 = file.path(base_root, "2022/dataset_03_2022/mom_comorbidities_list_ICD9_release.csv"),
      comorb_icd10= file.path(base_root, "2022/dataset_03_2022/mom_comorbidities_list_ICD10_release.csv"),
      bateman     = file.path(base_root, "2022/dataset_03_2022/mom_comorbidities_list_bateman_ICD9_release.csv"),
      maternal    = file.path(base_root, "2022/dataset_07_2022/maternal_release.csv")
    )
    
  } else if (site == "JAX") {
    
    files <- list(
      icd9        = file.path(base_root, "2025/dataset_04_2025/mom_diagnosis_ICD9_Jax.csv"),
      icd10       = file.path(base_root, "2025/dataset_04_2025/mom_diagnosis_ICD10_Jax.csv"),
      comorb_icd9 = file.path(base_root, "2025/dataset_04_2025/mom_comorbidities_list_ICD9_release_Jax.csv"),
      comorb_icd10= file.path(base_root, "2025/dataset_04_2025/mom_comorbidities_list_ICD10_release_Jax.csv"),
      bateman     = file.path(base_root, "2025/dataset_04_2025/mom_comorbidities_list_batman_ICD9_release_Jax.csv"),
      maternal    = file.path(base_root, "2025/dataset_04_2025/maternal_release_Jax.csv")
    )
    
  } else {
    stop("Unsupported site: ", site)
  }
  
  # ===============================
  # Check files exist
  # ===============================
  message("[", site, "] checking files...")
  for (nm in names(files)) {
    exists_flag <- file.exists(files[[nm]])
    message("[", site, "] ", files[[nm]], " | exists=", exists_flag)
  }
  
  # ===============================
  # Load primary ICD files
  # ===============================
  message("[", site, "] loading ICD9")
  icd9 <- read_csv(files$icd9, show_col_types = FALSE)
  
  message("[", site, "] loading ICD10")
  icd10 <- read_csv(files$icd10, show_col_types = FALSE)
  
  # ===============================
  # Standardize column names
  # ===============================
  icd9 <- standardize_icd_columns(icd9, site)
  icd10 <- standardize_icd_columns(icd10, site)
  
  # ===============================
  # FIX TYPE MISMATCH (CRITICAL)
  # ===============================
  icd9 <- icd9 %>%
    mutate(dx_code = as.character(dx_code))
  
  icd10 <- icd10 %>%
    mutate(dx_code = as.character(dx_code))
  
  message("[", site, "] columns after standardization:")
  print(names(icd9))
  print(names(icd10))
  
  # ===============================
  # Combine ICD9 + ICD10
  # ===============================
  df <- bind_rows(icd9, icd10)
  
  message("[", site, "] combined rows: ", nrow(df))
  
  # ===============================
  # Rename ID column before cleaning
  # ===============================
  df <- df %>%
    rename(part_id_mom = deidentified_mom_id)
  
  # ===============================
  # Clean ICD data
  # ===============================
  message("=== RUNNING clean_icd() ===")
  
  df <- clean_icd(
    df,
    id_col   = "part_id_mom",
    type_col = "dx_type",
    code_col = "dx_code",
    desc_col = "dx_descrip",
    date_col = "dx_date"
  )
  
  message("[", site, "] after clean_icd rows: ", nrow(df))
  
  ## -------------------------------------------------------------------------
  ## 🔧 STANDARDIZE MOM ID + ROBUST MERGE
  ## -------------------------------------------------------------------------
  
  # --- Step 1: Standardize maternal ID ---
  df <- df %>%
    mutate(
      part_id_mom = trimws(as.character(part_id_mom))
    )
  
  # --- Step 2: Prepare mom_baby_link ---
  mom_link_clean <- mom_baby_link_df %>%
    select(part_id_mom, delivery_id) %>%
    mutate(
      part_id_mom = trimws(as.character(part_id_mom))
    ) %>%
    distinct()
  
  # --- Step 3: Diagnostics ---
  anti <- df %>%
    anti_join(mom_link_clean, by = "part_id_mom")
  
  message("[", site, "] unmatched moms BEFORE merge: ", nrow(anti))
  
  # --- Step 4: Merge ---
  df <- df %>%
    inner_join(mom_link_clean, by = "part_id_mom")
  
  message("[", site, "] rows AFTER merge: ", nrow(df))
  
  # ===============================
  # Add site
  # ===============================
  df <- df %>%
    mutate(site = site)
  
  return(df)
}