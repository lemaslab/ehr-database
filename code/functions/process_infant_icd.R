process_infant_icd <- function(site,
                                         working_dir = getwd(),
                                         mom_baby_link_df = NULL) {
  
  message("=== PROCESSING INFANT ICD: ", site, " ===")
  
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
    library(stringr)
    library(janitor)
    library(purrr)
  })
  
  site <- toupper(site)
  
  # ===============================
  # Validate site
  # ===============================
  if (!site %in% c("GNV", "JAX")) {
    stop("Unsupported site: ", site)
  }
  
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
  # Helper: harmonize one infant ICD-like file
  # ===============================
  harmonize_infant_icd <- function(df, source_name, dx_category_value) {
    
    infant_id_col <- pick_col(
      df,
      c(
        "deidentified_baby_id",
        "deidentified_infant_id",
        "baby_id",
        "infant_id",
        "child_id",
        "part_id_infant"
      ),
      required = TRUE,
      label = paste0(source_name, " infant ID column")
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
        "diagnosis_start_datetime",
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
        deidentified_baby_id = as.character(.data[[infant_id_col]]),
        
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
          NA_character_
        },
        
        dx_date_raw = if (!is.null(dx_date_col)) {
          as.character(.data[[dx_date_col]])
        } else {
          NA_character_
        },
        
        dx_category = dx_category_value,
        icd_source = source_name,
        site = site
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
        deidentified_baby_id = trimws(deidentified_baby_id),
        dx_code              = str_to_upper(trimws(dx_code)),
        dx_descrip           = trimws(dx_descrip),
        dx_type              = trimws(dx_type),
        dx_icd_type          = str_to_upper(trimws(dx_icd_type))
      ) %>%
      filter(
        !is.na(deidentified_baby_id),
        deidentified_baby_id != ""
      ) %>%
      select(
        deidentified_baby_id,
        dx_category,
        dx_code,
        dx_descrip,
        dx_date,
        dx_date_raw,
        dx_type,
        dx_icd_type,
        icd_source,
        site
      )
  }
  
  # ===============================
  # Define file paths by site
  # ===============================
  base_root <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data"
  
  if (site == "GNV") {
    
    data_directory <- file.path(
      base_root,
      "2021/dataset_09_2021/baby_data"
    )
    
    file_map <- list(
      baby_asthma        = "asthma",
      baby_ear_infection = "ear_infection",
      baby_eczema        = "eczema",
      baby_food_allergy  = "food_allergy",
      baby_hemangonia    = "hemangonia",
      baby_nevus         = "nevus",
      baby_obesity       = "obesity",
      baby_sebor         = "sebor",
      baby_toxicum       = "toxicum"
    )
    
    files <- file.path(data_directory, paste0(names(file_map), ".csv"))
    names(files) <- names(file_map)
    
    missing_files <- files[!file.exists(files)]
    
    if (length(missing_files) > 0) {
      warning(
        "[", site, "] Missing GNV infant ICD file(s), skipping:\n",
        paste(missing_files, collapse = "\n")
      )
    }
    
    existing_files <- files[file.exists(files)]
    
    icd_list <- imap(
      existing_files,
      ~ read_clean(.x, .y) %>%
        harmonize_infant_icd(
          source_name = .y,
          dx_category_value = file_map[[.y]]
        )
    )
    
    neonatal_path <- file.path(
      base_root,
      "2022/dataset_07_2022/neonatal_defects_release.csv"
    )
    
    if (file.exists(neonatal_path)) {
      neonatal_df <- read_clean(
        path = neonatal_path,
        source_name = "neonatal_defects"
      ) %>%
        harmonize_infant_icd(
          source_name = "neonatal_defects",
          dx_category_value = "neonatal_defects"
        )
      
      icd_list <- c(icd_list, list(neonatal_df))
    } else {
      warning("[", site, "] missing neonatal defects file: ", neonatal_path)
    }
    
  } else if (site == "JAX") {
    
    neonatal_path <- file.path(
      base_root,
      "2025/dataset_04_2025/neonatal_defects_release_Jax.csv"
    )
    
    if (!file.exists(neonatal_path)) {
      stop("[", site, "] file not found: ", neonatal_path)
    }
    
    icd_list <- list(
      read_clean(
        path = neonatal_path,
        source_name = "neonatal_defects_jax"
      ) %>%
        harmonize_infant_icd(
          source_name = "neonatal_defects_jax",
          dx_category_value = "neonatal_defects"
        )
    )
  }
  
  # ===============================
  # Combine ICD files
  # ===============================
  if (length(icd_list) == 0) {
    stop("[", site, "] No infant ICD files available after file checks")
  }
  
  infant_icd <- bind_rows(icd_list)
  
  # ===============================
  # Standardize infant participant ID
  # Final output: part_id_infant = AC-infant-* or DC-infant-*
  # ===============================
  infant_icd <- standardize_participant_ids(
    df = infant_icd,
    site = site,
    mom_id_col = NULL,
    infant_id_col = "deidentified_baby_id"
  ) %>%
    distinct() %>%
    select(
      part_id_infant,
      dx_category,
      dx_code,
      dx_descrip,
      dx_date,
      dx_date_raw,
      dx_type,
      dx_icd_type,
      icd_source,
      site
    )
  
  # ===============================
  # Join to mom-baby link
  # ===============================
  if (!is.null(mom_baby_link_df)) {
    
    message("[", site, "] joining to mom_baby_link")
    
    required_mb_cols <- c("part_id_infant", "delivery_id", "part_dob_infant")
    missing_mb_cols <- setdiff(required_mb_cols, names(mom_baby_link_df))
    
    if (length(missing_mb_cols) > 0) {
      stop(
        "[", site, "] mom_baby_link_df missing required column(s): ",
        paste(missing_mb_cols, collapse = ", ")
      )
    }
    
    mb <- mom_baby_link_df %>%
      select(part_id_infant, delivery_id, part_dob_infant) %>%
      distinct()
    
    infant_icd <- infant_icd %>%
      left_join(mb, by = "part_id_infant") %>%
      mutate(
        dx_date = as_date(dx_date),
        part_dob_infant = as_date(part_dob_infant),
        dx2delivery_days = as.numeric(dx_date - part_dob_infant)
      ) %>%
      select(
        part_id_infant,
        delivery_id,
        dx_category,
        dx_code,
        dx_descrip,
        dx_date,
        dx_date_raw,
        dx2delivery_days,
        dx_type,
        dx_icd_type,
        icd_source,
        site
      )
    
  } else {
    warning("[", site, "] mom_baby_link_df not provided — skipping linkage")
  }
  
  # ===============================
  # QA
  # ===============================
  message("[", site, "] rows after cleanup: ", nrow(infant_icd))
  message("[", site, "] unique infants: ", dplyr::n_distinct(infant_icd$part_id_infant))
  
  if ("delivery_id" %in% names(infant_icd)) {
    message("[", site, "] missing delivery_id: ", sum(is.na(infant_icd$delivery_id)))
  }
  
  message("[", site, "] missing dx_code: ", sum(is.na(infant_icd$dx_code) | infant_icd$dx_code == ""))
  message("[", site, "] missing dx_date: ", sum(is.na(infant_icd$dx_date)))
  
  # ===============================
  # Return processed site-level dataset
  # Export is handled by run_infant_icd.R
  # ===============================
  return(infant_icd)
}