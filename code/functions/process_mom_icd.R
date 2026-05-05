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
    
    dx_code_col <- pick_col(
      df,
      c(
        "diagnosis_code",
        "dx_code",
        "icd_code",
        "icd9_code",
        "icd10_code",
        "code",
        "diagnosis_cd",
        "icd_cd"
      ),
      required = FALSE,
      label = paste0(source_name, " diagnosis code column")
    )
    
    dx_descrip_col <- pick_col(
      df,
      c(
        "diagnosis_description",
        "dx_descrip",
        "dx_description",
        "icd_desc",
        "icd_description",
        "description",
        "diagnosis_name",
        "dx_name"
      ),
      required = FALSE,
      label = paste0(source_name, " diagnosis description column")
    )
    
    dx_type_col <- pick_col(
      df,
      c(
        "diagnosis_type",
        "dx_type"
      ),
      required = FALSE,
      label = paste0(source_name, " diagnosis type column")
    )
    
    dx_icd_type_col <- pick_col(
      df,
      c(
        "icd_type",
        "dx_icd_type",
        "icd_version",
        "code_type"
      ),
      required = FALSE,
      label = paste0(source_name, " ICD type column")
    )
    
    dx_date_col <- pick_col(
      df,
      c(
        "deid_diagnosis_start_date",
        "diagnosis_start_date",
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
    
    df %>%
      mutate(
        deidentified_mom_id = as.character(.data[[mom_id_col]]),
        dx_code = if (!is.null(dx_code_col)) {
          as.character(.data[[dx_code_col]])
        } else {
          NA_character_
        },
        dx_descrip = if (!is.null(dx_descrip_col)) {
          as.character(.data[[dx_descrip_col]])
        } else {
          NA_character_
        },
        dx_type = if (!is.null(dx_type_col)) {
          as.character(.data[[dx_type_col]])
        } else {
          NA_character_
        },
        dx_icd_type = if (!is.null(dx_icd_type_col)) {
          as.character(.data[[dx_icd_type_col]])
        } else {
          case_when(
            str_detect(source_name, "icd9") ~ "ICD9",
            str_detect(source_name, "icd10") ~ "ICD10",
            TRUE ~ NA_character_
          )
        },
        dx_date_raw = if (!is.null(dx_date_col)) {
          as.character(.data[[dx_date_col]])
        } else {
          NA_character_
        },
        icd_source = source_name
      ) %>%
      mutate(
        dx_date = parse_date_time(
          dx_date_raw,
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
        dx_code             = str_to_upper(trimws(dx_code)),
        dx_descrip          = trimws(dx_descrip),
        dx_type             = trimws(dx_type),
        dx_icd_type         = str_to_upper(trimws(dx_icd_type)),
        site                = site
      ) %>%
      filter(
        !is.na(deidentified_mom_id),
        deidentified_mom_id != ""
      ) %>%
      select(
        deidentified_mom_id,
        dx_date,
        dx_code,
        dx_descrip,
        dx_type,
        dx_icd_type,
        icd_source,
        site
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
      dx_date,
      dx_date_raw,
      dx_code,
      dx_descrip,
      dx_type,
      dx_icd_type,
      icd_source,
      site
    )
  
  message("[", site, "] rows after cleanup: ", nrow(df))
  message("[", site, "] unique moms: ", dplyr::n_distinct(df$part_id_mom))
  message("[", site, "] missing dx_code: ", sum(is.na(df$dx_code) | df$dx_code == ""))
  message("[", site, "] missing dx_date: ", sum(is.na(df$dx_date)))
  
  # ===============================
  # Return processed site-level dataset
  # Export is handled by run_mom_icd.R
  # ===============================
  return(df)
}