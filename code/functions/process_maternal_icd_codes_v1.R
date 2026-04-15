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
  # FILE PATHS
  # ===============================
  
  if (site == "GNV") {
    
    base <- "V:/FACULTY/DJLEMAS/EHR_Data/raw/READ_ONLY_DATASETS/10year_data/"
    
    path_icd9  <- paste0(base, "2021/dataset_10_2021/mom_diagnosis_ICD9.csv")
    path_icd10 <- paste0(base, "2021/dataset_10_2021/mom_diagnosis_ICD10.csv")
    
    path_bateman <- paste0(base, "2022/dataset_03_2022/mom_comorbidities_list_bateman_ICD9_release.csv")
    path_comorb9 <- paste0(base, "2022/dataset_03_2022/mom_comorbidities_list_ICD9_release.csv")
    path_comorb10 <- paste0(base, "2022/dataset_03_2022/mom_comorbidities_list_ICD10_release.csv")
    
    path_maternal <- paste0(base, "2022/dataset_07_2022/maternal_release.csv")
  }
  
  if (site == "JAX") {
    
    base <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/"
    
    path_icd9  <- paste0(base, "mom_diagnosis_ICD9_Jax.csv")
    path_icd10 <- paste0(base, "mom_diagnosis_ICD10_Jax.csv")
    
    path_bateman <- paste0(base, "mom_comorbidities_list_bateman_ICD9_release_Jax.csv")
    path_comorb9 <- paste0(base, "mom_comorbidities_list_ICD9_release_Jax.csv")
    path_comorb10 <- paste0(base, "mom_comorbidities_list_ICD10_release_Jax.csv")
    
    path_maternal <- paste0(base, "maternal_release_Jax.csv")
  }
  
  paths <- c(
    path_icd9,
    path_icd10,
    path_bateman,
    path_comorb9,
    path_comorb10,
    path_maternal
  )
  
  message("[", site, "] checking input files")
  for (p in paths) {
    message("[", site, "] ", p, " | exists=", file.exists(p))
  }
  
  missing_files <- paths[!file.exists(paths)]
  if (length(missing_files) > 0) {
    stop("[", site, "] missing files:\n", paste(missing_files, collapse = "\n"))
  }
  
  # ===============================
  # HELPER FUNCTION
  # ===============================
  
  clean_icd <- function(df, id_col, type_col, code_col, desc_col, date_col, category) {
    df %>%
      rename(
        part_id_mom_tmp = !!rlang::sym(id_col),
        dx_type = !!rlang::sym(type_col),
        dx_code = !!rlang::sym(code_col),
        dx_descrip = !!rlang::sym(desc_col),
        dx_date = !!rlang::sym(date_col)
      ) %>%
      mutate(
        part_id_mom = paste0(site_prefix, part_id_mom_tmp),
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
  # LOAD + CLEAN
  # ===============================
  
  message("[", site, "] reading ICD9")
  raw_icd9 <- read_csv(path_icd9, show_col_types = FALSE, progress = TRUE)
  message("[", site, "] ICD9 raw rows: ", nrow(raw_icd9))
  mom_icd9 <- raw_icd9 %>%
    clean_icd(
      "deidentified_mom_id",
      "diagnosis_type",
      "diagnosis_code",
      "diagnosis_description",
      "diagnosis_start_date",
      "uncategorized"
    )
  message("[", site, "] ICD9 cleaned rows: ", nrow(mom_icd9))
  
  message("[", site, "] reading ICD10")
  raw_icd10 <- read_csv(path_icd10, show_col_types = FALSE, progress = TRUE)
  message("[", site, "] ICD10 raw rows: ", nrow(raw_icd10))
  mom_icd10 <- raw_icd10 %>%
    clean_icd(
      "deidentified_mom_id",
      "diagnosis_type",
      "diagnosis_code",
      "diagnosis_description",
      "diagnosis_start_date",
      "uncategorized"
    )
  message("[", site, "] ICD10 cleaned rows: ", nrow(mom_icd10))
  
  message("[", site, "] reading bateman")
  raw_bateman <- read_csv(path_bateman, show_col_types = FALSE, progress = TRUE)
  message("[", site, "] bateman raw rows: ", nrow(raw_bateman))
  bateman <- raw_bateman %>%
    clean_icd(
      "Deidentified_mom_ID",
      "Diagnosis Type",
      "Diagnosis Code",
      "Diagnosis Description",
      "Diagnosis Start Date",
      "bateman"
    )
  message("[", site, "] bateman cleaned rows: ", nrow(bateman))
  
  message("[", site, "] reading comorbidity ICD9")
  raw_comorb9 <- read_csv(path_comorb9, show_col_types = FALSE, progress = TRUE)
  message("[", site, "] comorbidity ICD9 raw rows: ", nrow(raw_comorb9))
  comorb9 <- raw_comorb9 %>%
    clean_icd(
      "Deidentified_mom_ID",
      "Diagnosis Type",
      "Diagnosis Code",
      "Diagnosis Description",
      "Diagnosis Start Date",
      "comorbidities"
    )
  message("[", site, "] comorbidity ICD9 cleaned rows: ", nrow(comorb9))
  
  message("[", site, "] reading comorbidity ICD10")
  raw_comorb10 <- read_csv(path_comorb10, show_col_types = FALSE, progress = TRUE)
  message("[", site, "] comorbidity ICD10 raw rows: ", nrow(raw_comorb10))
  comorb10 <- raw_comorb10 %>%
    clean_icd(
      "Deidentified_mom_ID",
      "Diagnosis Type",
      "Diagnosis Code",
      "Diagnosis Description",
      "Diagnosis Start Date",
      "comorbidities"
    )
  message("[", site, "] comorbidity ICD10 cleaned rows: ", nrow(comorb10))
  
  message("[", site, "] reading maternal release")
  raw_maternal <- read_csv(path_maternal, show_col_types = FALSE, progress = TRUE)
  message("[", site, "] maternal release raw rows: ", nrow(raw_maternal))
  maternal <- raw_maternal %>%
    clean_icd(
      "Deidentified_mom_ID",
      "Diagnosis Type",
      "Diagnosis Code",
      "Diagnosis Description",
      "Diagnosis Start Date",
      "maternal_release"
    )
  message("[", site, "] maternal release cleaned rows: ", nrow(maternal))
  
  # ===============================
  # COMBINE
  # ===============================
  
  message("[", site, "] combining datasets")
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
  # JOIN MOM-BABY LINK (OPTIONAL)
  # ===============================
  
  if (!is.null(mom_baby_link_df)) {
    message("[", site, "] joining mom_baby_link")
    maternal_icd <- maternal_icd %>%
      left_join(mom_baby_link_df, by = "part_id_mom")
    message("[", site, "] rows after join: ", nrow(maternal_icd))
  } else {
    warning("[", site, "] mom_baby_link_df not provided")
  }
  
  # ===============================
  # EXPORT (SITE LEVEL)
  # ===============================
  
  date_tag <- format(Sys.Date(), "%Y%m%d")
  
  out_dir <- file.path(working_dir, "data", "processed", site)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  file_base <- paste0("maternal_icd_", site, "_", date_tag)
  
  message("[", site, "] writing outputs")
  save(maternal_icd, file = file.path(out_dir, paste0(file_base, ".rda")))
  write_csv(maternal_icd, file.path(out_dir, paste0(file_base, ".csv")), na = "")
  
  message("[", site, "] done")
  return(maternal_icd)
}