process_maternal_icd_codes_v1 <- function(site, working_dir, mom_baby_link_df) {
  
  message("=== PROCESSING MATERNAL ICD: ", site, " ===")
  
  library(dplyr)
  library(readr)
  library(lubridate)
  library(rlang)
  
  # ===============================
  # Helper: rename if column exists
  # ===============================
  rename_if_exists <- function(df, new, old) {
    if (old %in% names(df)) {
      df <- df %>% rename(!!new := !!sym(old))
    }
    return(df)
  }
  
  # ===============================
  # Standardize columns
  # ===============================
  standardize_icd_columns <- function(df, site) {
    
    names(df) <- tolower(names(df))
    
    message("[", site, "] standardizing ICD column names")
    
    df <- df %>%
      rename_if_exists("dx_type", "diagnosis_type") %>%
      rename_if_exists("dx_code", "diagnosis_code") %>%
      rename_if_exists("dx_descrip", "diagnosis_description") %>%
      rename_if_exists("dx_date", "diagnosis_start_date")
    
    message("[", site, "] columns after standardization:")
    print(names(df))
    
    return(df)
  }
  
  # ===============================
  # Validate columns
  # ===============================
  validate_icd_columns <- function(df, site) {
    
    required_cols <- c("dx_type", "dx_code", "dx_descrip", "dx_date")
    
    missing_cols <- setdiff(required_cols, names(df))
    
    if (length(missing_cols) > 0) {
      stop("[", site, "] Missing columns AFTER standardization: ",
           paste(missing_cols, collapse = ", "))
    }
  }
  
  # ===============================
  # File paths
  # ===============================
  base_path <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/"
  
  if (site == "GNV") {
    
    files <- list(
      icd9  = file.path(base_path, "2021/dataset_10_2021/mom_diagnosis_ICD9.csv"),
      icd10 = file.path(base_path, "2021/dataset_10_2021/mom_diagnosis_ICD10.csv")
    )
    
  } else if (site == "JAX") {
    
    files <- list(
      icd9  = file.path(base_path, "2025/dataset_04_2025/mom_diagnosis_ICD9_Jax.csv"),
      icd10 = file.path(base_path, "2025/dataset_04_2025/mom_diagnosis_ICD10_Jax.csv")
    )
    
  } else {
    stop("Unsupported site: ", site)
  }
  
  # ===============================
  # Load data
  # ===============================
  message("[", site, "] loading ICD9")
  icd9 <- read_csv(files$icd9, show_col_types = FALSE)
  
  message("[", site, "] loading ICD10")
  icd10 <- read_csv(files$icd10, show_col_types = FALSE)
  
  # ===============================
  # Standardize
  # ===============================
  icd9  <- standardize_icd_columns(icd9, site)
  icd10 <- standardize_icd_columns(icd10, site)
  
  # 🔥 FIX: force consistent types
  icd9$dx_code  <- as.character(icd9$dx_code)
  icd10$dx_code <- as.character(icd10$dx_code)
  
  # ===============================
  # Validate
  # ===============================
  validate_icd_columns(icd9, site)
  validate_icd_columns(icd10, site)
  
  # ===============================
  # Combine
  # ===============================
  df <- bind_rows(icd9, icd10)
  
  message("[", site, "] combined rows: ", nrow(df))
  
  # ===============================
  # Clean ICD
  # ===============================
  df <- clean_icd(
    df,
    id_col   = "deidentified_mom_id",
    type_col = "dx_type",
    code_col = "dx_code",
    desc_col = "dx_descrip",
    date_col = "dx_date"
  )
  
  message("[", site, "] after clean_icd rows: ", nrow(df))
  
  # ===============================
  # Join
  # ===============================
  mom_link_clean <- mom_baby_link_df %>%
    rename(part_id_mom = deidentified_mom_id) %>%
    select(part_id_mom, delivery_id) %>%
    distinct()
  
  df <- df %>%
    rename(part_id_mom = deidentified_mom_id)
  
  df <- df %>%
    inner_join(mom_link_clean, by = "part_id_mom")
  
  message("[", site, "] after join rows: ", nrow(df))
  
  # ===============================
  # Add site
  # ===============================
  df <- df %>%
    mutate(site = site)
  
  message("[", site, "] COMPLETE")
  
  return(df)
}