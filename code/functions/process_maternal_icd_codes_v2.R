process_maternal_icd_codes_v2 <- function(site, working_dir, mom_baby_link_df = NULL) {
  
  message("=== PROCESSING MATERNAL ICD V2: ", site, " ===")
  
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
    if (!exists_flag && nm %in% c("icd9", "icd10")) {
      stop("[", site, "] Missing required file: ", files[[nm]])
    }
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
  # Harmonize types before bind_rows
  # ===============================
  icd9 <- icd9 %>%
    mutate(
      deidentified_mom_id = as.character(deidentified_mom_id),
      dx_code = as.character(dx_code),
      dx_descrip = as.character(dx_descrip),
      dx_type = as.character(dx_type),
      dx_date = lubridate::parse_date_time(
        dx_date,
        orders = c("ymd", "mdy", "dmy", "Ymd"),
        quiet = TRUE
      )
    )
  
  icd10 <- icd10 %>%
    mutate(
      deidentified_mom_id = as.character(deidentified_mom_id),
      dx_code = as.character(dx_code),
      dx_descrip = as.character(dx_descrip),
      dx_type = as.character(dx_type),
      dx_date = lubridate::parse_date_time(
        dx_date,
        orders = c("ymd", "mdy", "dmy", "Ymd"),
        quiet = TRUE
      )
    )
  
  message("[", site, "] columns after standardization:")
  print(names(icd9))
  print(names(icd10))
  
  # ===============================
  # Combine ICD9 + ICD10
  # ===============================
  df <- bind_rows(icd9, icd10)
  
  message("[", site, "] combined rows: ", nrow(df))
  
  # ===============================
  # Basic cleanup only
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id = trimws(as.character(deidentified_mom_id)),
      
      # ===============================
      # ADD STANDARDIZED MOM ID
      # ===============================
      part_id_mom = case_when(
        site == "GNV" ~ paste0("AC-mom-", deidentified_mom_id),
        site == "JAX" ~ paste0("DC-mom-", deidentified_mom_id),
        TRUE ~ deidentified_mom_id
      ),
      
      dx_code = trimws(as.character(dx_code)),
      dx_descrip = trimws(as.character(dx_descrip)),
      dx_type = trimws(as.character(dx_type)),
      dx_date = as.Date(dx_date),
      site = site
    ) %>%
    filter(
      !is.na(deidentified_mom_id),
      deidentified_mom_id != "",
      !is.na(dx_code),
      dx_code != ""
    ) %>%
    distinct() %>%
    filter(
      !is.na(deidentified_mom_id),
      deidentified_mom_id != "",
      !is.na(dx_code),
      dx_code != ""
    ) %>%
    distinct()
  
  message("[", site, "] rows after cleanup: ", nrow(df))
  message("[", site, "] unique moms: ", dplyr::n_distinct(df$deidentified_mom_id))
  message("[", site, "] missing dx_date: ", sum(is.na(df$dx_date)))
  
  return(df)
}