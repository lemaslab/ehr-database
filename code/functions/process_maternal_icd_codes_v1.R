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
  df <- clean_icd(df)
  
  message("[", site, "] after clean_icd rows: ", nrow(df))
  
  # ===============================
  # Prepare mom_baby_link
  # ===============================
  mom_link_clean <- mom_baby_link_df %>%
    select(part_id_mom, delivery_id) %>%
    distinct() %>%
    mutate(part_id_mom = as.character(part_id_mom))
  
  # ===============================
  # Ensure ID types match
  # ===============================
  df <- df %>%
    mutate(part_id_mom = as.character(part_id_mom))
  
  message("[", site, "] unique moms in mom_baby_link: ",
          dplyr::n_distinct(mom_link_clean$part_id_mom))
  
  message("[", site, "] unique moms in ICD: ",
          dplyr::n_distinct(df$part_id_mom))
  
  # ===============================
  # Join to mom_baby_link
  # ===============================
  message("[", site, "] joining mom_baby_link")
  
  df <- df %>%
    inner_join(mom_link_clean, by = "part_id_mom")
  
  message("[", site, "] after join rows: ", nrow(df))
  
  # ===============================
  # Add site
  # ===============================
  df <- df %>%
    mutate(site = site)
  
  return(df)
}