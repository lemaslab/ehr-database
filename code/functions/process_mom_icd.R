process_mom_icd <- function(site, working_dir) {
  
  message("=== PROCESSING MOM ICD: ", site, " ===")
  
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
    library(stringr)
    library(janitor)
    library(purrr)
  })
  
  # ===============================
  # Source shared ID standardization utility
  # ===============================
  source(file.path(working_dir, "code", "functions", "utils_standardize_ids.R"))
  
  # ===============================
  # Define file paths by site
  # ===============================
  base_root <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data"
  
  if (site == "GNV") {
    files <- list(
      icd9         = file.path(base_root, "2021/dataset_10_2021/mom_diagnosis_ICD9.csv"),
      icd10        = file.path(base_root, "2021/dataset_10_2021/mom_diagnosis_ICD10.csv"),
      comorb_icd9  = file.path(base_root, "2022/dataset_03_2022/mom_comorbidities_list_ICD9_release.csv"),
      comorb_icd10 = file.path(base_root, "2022/dataset_03_2022/mom_comorbidities_list_ICD10_release.csv"),
      bateman      = file.path(base_root, "2022/dataset_03_2022/mom_comorbidities_list_bateman_ICD9_release.csv"),
      maternal     = file.path(base_root, "2022/dataset_07_2022/maternal_release.csv")
    )
  } else if (site == "JAX") {
    files <- list(
      icd9         = file.path(base_root, "2025/dataset_04_2025/mom_diagnosis_ICD9_Jax.csv"),
      icd10        = file.path(base_root, "2025/dataset_04_2025/mom_diagnosis_ICD10_Jax.csv"),
      comorb_icd9  = file.path(base_root, "2025/dataset_04_2025/mom_comorbidities_list_ICD9_release_Jax.csv"),
      comorb_icd10 = file.path(base_root, "2025/dataset_04_2025/mom_comorbidities_list_ICD10_release_Jax.csv"),
      bateman      = file.path(base_root, "2025/dataset_04_2025/mom_comorbidities_list_batman_ICD9_release_Jax.csv"),
      maternal     = file.path(base_root, "2025/dataset_04_2025/maternal_release_Jax.csv")
    )
  } else {
    stop("Unsupported site: ", site)
  }
  
  # ===============================
  # Check files exist
  # ===============================
  missing_files <- files[!file.exists(unlist(files))]
  
  if (length(missing_files) > 0) {
    stop(
      "[", site, "] Missing file(s):\n",
      paste(unlist(missing_files), collapse = "\n")
    )
  }
  
  message("[", site, "] all ICD files found")
  
  # ===============================
  # Helper: clean names
  # ===============================
  read_clean <- function(path, source_name) {
    message("[", site, "] reading ", source_name, ": ", path)
    
    df <- read_csv(path, show_col_types = FALSE, progress = FALSE)
    names(df) <- janitor::make_clean_names(names(df))
    
    df %>%
      mutate(
        source_file_type = source_name
      )
  }
  
  # ===============================
  # Helper: safe column picker
  # ===============================
  pick_col <- function(df, candidates, required = FALSE, label = "column") {
    existing <- intersect(candidates, names(df))
    if (length(existing) == 0) {
      if (required) {
        stop(
          "[", site, "] Missing required ", label, ". Tried: ",
          paste(candidates, collapse = ", "),
          ". Available columns: ", paste(names(df), collapse = ", ")
        )
      }
      return(NULL)
    }
    return(existing[1])
  }
  
  # ===============================
  # Helper: harmonize one ICD-like file
  # ===============================
  harmonize_icd <- function(df, source_name) {
    
    mom_id_col <- pick_col(
      df,
      c(
        "deidentified_mom_id",
        "mom_id",
        "maternal_id",
        "mother_id",
        "part_id_mom"
      ),
      required = TRUE,
      label = paste0(source_name, " mom ID column")
    )
    
    icd_code_col <- pick_col(
      df,
      c(
        "icd_code",
        "icd9_code",
        "icd10_code",
        "diagnosis_code",
        "dx_code",
        "code",
        "diagnosis_cd",
        "icd_cd"
      ),
      required = FALSE,
      label = paste0(source_name, " ICD code column")
    )
    
    icd_desc_col <- pick_col(
      df,
      c(
        "icd_desc",
        "icd_description",
        "diagnosis_description",
        "dx_description",
        "description",
        "diagnosis_name",
        "dx_name"
      ),
      required = FALSE,
      label = paste0(source_name, " ICD description column")
    )
    
    date_dx_col <- pick_col(
      df,
      c(
        "date_dx",
        "dx_date",
        "diagnosis_date",
        "diagnosis_datetime",
        "encounter_date",
        "admit_date",
        "contact_date",
        "start_date"
      ),
      required = FALSE,
      label = paste0(source_name, " diagnosis date column")
    )
    
    encounter_col <- pick_col(
      df,
      c(
        "encounter_id",
        "encounter",
        "deidentified_encounter_id",
        "enc_id",
        "visit_id"
      ),
      required = FALSE,
      label = paste0(source_name, " encounter ID column")
    )
    
    df %>%
      mutate(
        deidentified_mom_id = as.character(.data[[mom_id_col]]),
        icd_code = if (!is.null(icd_code_col)) {
          as.character(.data[[icd_code_col]])
        } else {
          NA_character_
        },
        icd_desc = if (!is.null(icd_desc_col)) {
          as.character(.data[[icd_desc_col]])
        } else {
          NA_character_
        },
        date_dx_raw = if (!is.null(date_dx_col)) {
          as.character(.data[[date_dx_col]])
        } else {
          NA_character_
        },
        encounter_id = if (!is.null(encounter_col)) {
          as.character(.data[[encounter_col]])
        } else {
          NA_character_
        },
        icd_source = source_name,
        icd_version = case_when(
          str_detect(source_name, "icd9") ~ "ICD9",
          str_detect(source_name, "icd10") ~ "ICD10",
          TRUE ~ NA_character_
        )
      ) %>%
      mutate(
        date_dx = parse_date_time(
          date_dx_raw,
          orders = c(
            "ymd HMS", "ymd HM", "ymd",
            "mdy HMS", "mdy HM", "mdy",
            "dmy HMS", "dmy HM", "dmy",
            "Ymd HMS", "Ymd HM", "Ymd",
            "m/d/Y H:M:S", "m/d/Y H:M", "m/d/Y",
            "m/d/y H:M:S", "m/d/y H:M", "m/d/y"
          ),
          quiet = TRUE
        )
      ) %>%
      mutate(
        deidentified_mom_id = trimws(deidentified_mom_id),
        icd_code            = str_to_upper(trimws(icd_code)),
        icd_desc            = trimws(icd_desc),
        encounter_id        = trimws(encounter_id),
        site                = site
      ) %>%
      filter(
        !is.na(deidentified_mom_id),
        deidentified_mom_id != ""
      )
  }
  
  # ===============================
  # Read ICD/comorbidity files
  # ===============================
  icd_dfs <- imap(
    files[names(files) != "maternal"],
    ~ read_clean(.x, .y) %>%
      harmonize_icd(source_name = .y)
  )
  
  df <- bind_rows(icd_dfs)
  
  # ===============================
  # Standardize maternal participant ID
  # Final output: part_id_mom = AC-mom-* or DC-mom-*
  # ===============================
  df <- standardize_participant_ids(
    df = df,
    site = site,
    mom_id_col = "deidentified_mom_id",
    infant_id_col = NULL
  ) %>%
    distinct() %>%
    select(
      part_id_mom,
      icd_code,
      icd_desc,
      icd_version,
      icd_source,
      date_dx,
      date_dx_raw,
      encounter_id,
      site
    )
  
  message("[", site, "] rows after cleanup: ", nrow(df))
  message("[", site, "] unique moms: ", dplyr::n_distinct(df$part_id_mom))
  message("[", site, "] missing icd_code: ", sum(is.na(df$icd_code) | df$icd_code == ""))
  message("[", site, "] missing date_dx: ", sum(is.na(df$date_dx)))
  
  # ===============================
  # Return processed site-level dataset
  # Export is handled by run_mom_icd.R
  # ===============================
  return(df)
}