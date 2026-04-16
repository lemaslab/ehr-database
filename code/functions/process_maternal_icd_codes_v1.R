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
    mutate(
      dx_code = as.character(dx_code),
      dx_date = lubridate::parse_date_time(
        dx_date,
        orders = c("ymd", "mdy", "dmy", "Ymd"),
        quiet = TRUE
      )
    )
  
  icd10 <- icd10 %>%
    mutate(
      dx_code = as.character(dx_code),
      dx_date = lubridate::parse_date_time(
        dx_date,
        orders = c("ymd", "mdy", "dmy", "Ymd"),
        quiet = TRUE
      )
    )
  
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
  ## 🔧 FIX ID MAPPING USING MATERNAL CROSSWALK (CRITICAL)
  ## -------------------------------------------------------------------------
  
  # --- Step 1: Load maternal crosswalk ---
  message("[", site, "] loading maternal crosswalk")
  
  maternal <- read_csv(files$maternal, show_col_types = FALSE)
  
  # ===============================
  # STANDARDIZE COLUMN NAMES (CRITICAL FIX)
  # ===============================
  names(maternal) <- tolower(names(maternal))
  names(maternal) <- gsub(" ", "_", names(maternal))
  
  message("[", site, "] maternal columns:")
  print(names(maternal))
  
  # ===============================
  # ENSURE REQUIRED ID COLUMN EXISTS
  # ===============================
  if (!"deidentified_mom_id" %in% names(maternal)) {
    stop("[", site, "] maternal file missing deidentified_mom_id after standardization")
  }
  
  # ===============================
  # FIX ID TYPES
  # ===============================
  maternal <- maternal %>%
    mutate(
      deidentified_mom_id = as.character(deidentified_mom_id)
    )
  
  # --- Step 3: Standardize ICD ID ---
  df <- df %>%
    mutate(
      deidentified_mom_id = as.character(part_id_mom)
    )
  
  # --- Step 4: Join ICD → maternal (CRITICAL STEP) ---
  df <- df %>%
    inner_join(maternal, by = "deidentified_mom_id")
  
  # ===============================
  # 🔧 FIX ID FORMAT (CRITICAL)
  # ===============================
  if (site == "GNV") {
    df <- df %>%
      mutate(part_id_mom = paste0("AC-mom-", part_id_mom))
  }
  
  if (site == "JAX") {
    df <- df %>%
      mutate(part_id_mom = paste0("DC-mom-", part_id_mom))
  }
  
  # ===============================
  # Prepare mom_baby_link
  # ===============================
  mom_link_clean <- mom_baby_link_df %>%
    select(part_id_mom, delivery_id) %>%
    mutate(
      part_id_mom = trimws(as.character(part_id_mom))
    ) %>%
    distinct()
  
  # ===============================
  # Ensure df ID type matches
  # ===============================
  df <- df %>%
    mutate(
      part_id_mom = trimws(as.character(part_id_mom))
    )
  
  # ===============================
  # Final join
  # ===============================
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