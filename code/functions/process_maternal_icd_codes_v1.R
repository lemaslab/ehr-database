process_maternal_icd_codes_v1 <- function(site,
                                          working_dir = getwd(),
                                          mom_baby_link_df = NULL) {
  
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
  })
  
  site <- toupper(site)
  
  if (!site %in% c("GNV", "JAX")) {
    stop("Unsupported site")
  }
  
  message("=== PROCESSING MATERNAL ICD: ", site, " ===")
  
  # ===============================
  # SITE MAP / ID PREFIX
  # ===============================
  
  site_map <- c("GNV" = "AC", "JAX" = "DC")
  site_prefix <- paste0(site_map[[site]], "-mom-")
  
  # ===============================
  # BASE PATH
  # ===============================
  
  ehr_base <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/"
  
  # ===============================
  # FILE PATHS
  # ===============================
  
  if (site == "GNV") {
    
    path_icd9  <- file.path(ehr_base, "2021/dataset_10_2021/mom_diagnosis_ICD9.csv")
    path_icd10 <- file.path(ehr_base, "2021/dataset_10_2021/mom_diagnosis_ICD10.csv")
    
    path_bateman <- file.path(ehr_base, "2022/dataset_03_2022/mom_comorbidities_list_bateman_ICD9_release.csv")
    path_comorb9 <- file.path(ehr_base, "2022/dataset_03_2022/mom_comorbidities_list_ICD9_release.csv")
    path_comorb10 <- file.path(ehr_base, "2022/dataset_03_2022/mom_comorbidities_list_ICD10_release.csv")
    
    path_maternal <- file.path(ehr_base, "2022/dataset_07_2022/maternal_release.csv")
  }
  
  if (site == "JAX") {
    
    jax_base <- file.path(ehr_base, "2025/dataset_04_2025")
    
    path_icd9  <- file.path(jax_base, "mom_diagnosis_ICD9_Jax.csv")
    path_icd10 <- file.path(jax_base, "mom_diagnosis_ICD10_Jax.csv")
    
    path_bateman <- file.path(jax_base, "mom_comorbidities_list_bateman_ICD9_release_Jax.csv")
    path_comorb9 <- file.path(jax_base, "mom_comorbidities_list_ICD9_release_Jax.csv")
    path_comorb10 <- file.path(jax_base, "mom_comorbidities_list_ICD10_release_Jax.csv")
    
    path_maternal <- file.path(jax_base, "maternal_release_Jax.csv")
  }
  
  paths <- c(path_icd9, path_icd10, path_bateman, path_comorb9, path_comorb10, path_maternal)
  
  # ===============================
  # VALIDATE FILES
  # ===============================
  
  message("[", site, "] checking files...")
  for (p in paths) {
    message("[", site, "] ", p, " | exists=", file.exists(p))
  }
  
  missing_files <- paths[!file.exists(paths)]
  if (length(missing_files) > 0) {
    stop("[", site, "] Missing files:\n", paste(missing_files, collapse = "\n"))
  }
  
  # ===============================
  # HELPER (SAFE RENAMING)
  # ===============================
  
  clean_icd <- function(df, id_col, type_col, code_col, desc_col, date_col, category) {
    
    # SAFE renaming (no NSE issues)
    names(df)[names(df) == id_col]   <- "part_id_mom_tmp"
    names(df)[names(df) == type_col] <- "dx_type"
    names(df)[names(df) == code_col] <- "dx_code"
    names(df)[names(df) == desc_col] <- "dx_descrip"
    names(df)[names(df) == date_col] <- "dx_date"
    
    df %>%
      mutate(
        part_id_mom = paste0(site_prefix, part_id_mom_tmp),
        
        # enforce consistent types
        dx_code = as.character(dx_code),
        dx_descrip = as.character(dx_descrip),
        dx_type = as.character(dx_type),
        
        dx_category = category
      ) %>%
      select(-part_id_mom_tmp) %>%
      mutate(
        dx_date = suppressWarnings(parse_date_time(
          dx_date,
          orders = c("mdy", "ymd", "mdy HMS", "ymd HMS")
        ))
      ) %>%
      select(part_id_mom, dx_date, dx_category, dx_code, dx_descrip, dx_type)
  }
  
  # ===============================
  # LOAD + PROCESS
  # ===============================
  
  message("[", site, "] loading ICD9")
  mom_icd9 <- read_csv(path_icd9, show_col_types = FALSE) %>%
    clean_icd("deidentified_mom_id","diagnosis_type","diagnosis_code","diagnosis_description","diagnosis_start_date","uncategorized")
  
  message("[", site, "] loading ICD10")
  mom_icd10 <- read_csv(path_icd10, show_col_types = FALSE) %>%
    clean_icd("deidentified_mom_id","diagnosis_type","diagnosis_code","diagnosis_description","diagnosis_start_date","uncategorized")
  
  message("[", site, "] loading bateman")
  bateman <- read_csv(path_bateman, show_col_types = FALSE) %>%
    clean_icd("Deidentified_mom_ID","Diagnosis Type","Diagnosis Code","Diagnosis Description","Diagnosis Start Date","bateman")
  
  message("[", site, "] loading comorbidity ICD9")
  comorb9 <- read_csv(path_comorb9, show_col_types = FALSE) %>%
    clean_icd("Deidentified_mom_ID","Diagnosis Type","Diagnosis Code","Diagnosis Description","Diagnosis Start Date","comorbidities")
  
  message("[", site, "] loading comorbidity ICD10")
  comorb10 <- read_csv(path_comorb10, show_col_types = FALSE) %>%
    clean_icd("Deidentified_mom_ID","Diagnosis Type","Diagnosis Code","Diagnosis Description","Diagnosis Start Date","comorbidities")
  
  message("[", site, "] loading maternal release")
  maternal <- read_csv(path_maternal, show_col_types = FALSE) %>%
    clean_icd("Deidentified_mom_ID","Diagnosis Type","Diagnosis Code","Diagnosis Description","Diagnosis Start Date","maternal_release")
  
  # ===============================
  # COMBINE
  # ===============================
  
  maternal_icd <- bind_rows(
    mom_icd9,
    mom_icd10,
    bateman,
    comorb9,
    comorb10,
    maternal
  ) %>%
    mutate(site = site) %>%
    distinct()
  
  message("[", site, "] combined rows: ", nrow(maternal_icd))
  
  # ===============================
  # SAFE JOIN
  # ===============================
  
  if (!is.null(mom_baby_link_df)) {
    message("[", site, "] joining mom_baby_link")
    
    maternal_icd <- maternal_icd %>%
      left_join(
        mom_baby_link_df %>% select(part_id_mom, delivery_id),
        by = "part_id_mom"
      )
  }
  
  # ===============================
  # EXPORT
  # ===============================
  
  date_tag <- format(Sys.Date(), "%Y%m%d")
  
  out_dir <- file.path(working_dir, "data", "processed", site)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  file_base <- paste0("maternal_icd_", site, "_", date_tag)
  
  save(maternal_icd, file = file.path(out_dir, paste0(file_base, ".rda")))
  write_csv(maternal_icd, file.path(out_dir, paste0(file_base, ".csv")), na = "")
  
  message("[", site, "] COMPLETE")
  
  return(maternal_icd)
}