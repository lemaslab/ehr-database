process_mom_medications_ip_v2 <- function(site, working_dir) {
  
  message("=== PROCESSING MOM MEDICATIONS IP V2: ", site, " ===")
  
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
    library(stringr)
    library(janitor)
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
    file <- file.path(base_root, "2021/dataset_09_2021/mom_medication/mom_IP.csv")
  } else if (site == "JAX") {
    file <- file.path(base_root, "2025/dataset_04_2025/mom_IP_Jax.csv")
  } else {
    stop("Unsupported site: ", site)
  }
  
  # ===============================
  # Check file exists
  # ===============================
  message("[", site, "] checking file...")
  if (!file.exists(file)) {
    stop("[", site, "] Missing file: ", file)
  }
  message("[", site, "] file found")
  
  # ===============================
  # Load data
  # ===============================
  message("[", site, "] loading medication file")
  df <- read_csv(file, show_col_types = FALSE, progress = FALSE)
  
  message("[", site, "] rows: ", nrow(df))
  message("[", site, "] raw columns:")
  print(names(df))
  
  # ===============================
  # Standardize column names
  # Raw expected columns:
  # Deidentified_mom_ID, Med Order Display Name,
  # Medication History Category, Med Status Category,
  # Taken Datetime, Med Therapy Class, Pharmacy Class,
  # Pharmacy Subclass, Rxnorm Code
  # ===============================
  names(df) <- janitor::make_clean_names(names(df))
  
  message("[", site, "] cleaned columns:")
  print(names(df))
  
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
  
  mom_id_col <- pick_col(
    df,
    c("deidentified_mom_id", "mom_id", "part_id_mom"),
    required = TRUE,
    label = "mom ID column"
  )
  
  med_name_col <- pick_col(
    df,
    c("med_order_display_name", "med_name", "medication_name"),
    required = TRUE,
    label = "medication name column"
  )
  
  med_hist_cat_col <- pick_col(
    df,
    c("medication_history_category", "med_history_category"),
    required = FALSE,
    label = "medication history category column"
  )
  
  med_status_cat_col <- pick_col(
    df,
    c("med_status_category", "medication_status_category"),
    required = FALSE,
    label = "medication status category column"
  )
  
  date_col <- pick_col(
    df,
    c("taken_datetime", "taken_date", "start_date", "med_start_date"),
    required = FALSE,
    label = "taken datetime column"
  )
  
  med_therapy_class_col <- pick_col(
    df,
    c("med_therapy_class", "medication_therapy_class"),
    required = FALSE,
    label = "med therapy class column"
  )
  
  pharmacy_class_col <- pick_col(
    df,
    c("pharmacy_class", "pharm_class"),
    required = FALSE,
    label = "pharmacy class column"
  )
  
  pharmacy_subclass_col <- pick_col(
    df,
    c("pharmacy_subclass", "pharm_subclass"),
    required = FALSE,
    label = "pharmacy subclass column"
  )
  
  med_code_col <- pick_col(
    df,
    c("rxnorm_code", "rxnorm", "med_code"),
    required = FALSE,
    label = "rxnorm code column"
  )
  
  # ===============================
  # Harmonize variables
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id          = as.character(.data[[mom_id_col]]),
      med_name                     = as.character(.data[[med_name_col]]),
      medication_history_category  = if (!is.null(med_hist_cat_col)) as.character(.data[[med_hist_cat_col]]) else NA_character_,
      med_status_category          = if (!is.null(med_status_cat_col)) as.character(.data[[med_status_cat_col]]) else NA_character_,
      taken_datetime_raw           = if (!is.null(date_col)) as.character(.data[[date_col]]) else NA_character_,
      med_therapy_class            = if (!is.null(med_therapy_class_col)) as.character(.data[[med_therapy_class_col]]) else NA_character_,
      pharmacy_class               = if (!is.null(pharmacy_class_col)) as.character(.data[[pharmacy_class_col]]) else NA_character_,
      pharmacy_subclass            = if (!is.null(pharmacy_subclass_col)) as.character(.data[[pharmacy_subclass_col]]) else NA_character_,
      rxnorm_code                  = if (!is.null(med_code_col)) as.character(.data[[med_code_col]]) else NA_character_
    ) %>%
    mutate(
      taken_datetime = parse_date_time(
        taken_datetime_raw,
        orders = c(
          "ymd HMS", "ymd HM", "ymd",
          "mdy HMS", "mdy HM", "mdy",
          "dmy HMS", "dmy HM", "dmy",
          "Ymd HMS", "Ymd HM", "Ymd",
          "m/d/Y H:M:S", "m/d/Y H:M", "m/d/Y",
          "m/d/y H:M:S", "m/d/y H:M", "m/d/y"
        ),
        quiet = TRUE
      ),
      start_date = as.Date(taken_datetime)
    )
  
  # ===============================
  # Cleanup
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id         = trimws(deidentified_mom_id),
      med_name                    = str_to_upper(trimws(med_name)),
      medication_history_category = trimws(medication_history_category),
      med_status_category         = trimws(med_status_category),
      med_therapy_class           = trimws(med_therapy_class),
      pharmacy_class              = trimws(pharmacy_class),
      pharmacy_subclass           = trimws(pharmacy_subclass),
      rxnorm_code                 = trimws(rxnorm_code),
      site                        = site
    ) %>%
    filter(
      !is.na(deidentified_mom_id),
      deidentified_mom_id != "",
      !is.na(med_name),
      med_name != ""
    )
  
  # ===============================
  # STANDARDIZED ID
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
      start_date,
      taken_datetime,
      taken_datetime_raw,
      med_name,
      medication_history_category,
      med_status_category,
      med_therapy_class,
      pharmacy_class,
      pharmacy_subclass,
      rxnorm_code,
      site
    )
  
  message("[", site, "] rows after cleanup: ", nrow(df))
  message("[", site, "] unique moms: ", dplyr::n_distinct(df$part_id_mom))
  message("[", site, "] missing start_date: ", sum(is.na(df$start_date)))
  
  # ===============================
  # EXPORT (SITE-SPECIFIC)
  # ===============================
  output_dir <- file.path(working_dir, "data", "processed", site)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  
  date_tag <- format(Sys.Date(), "%Y%m%d")
  
  rds_file <- file.path(output_dir, paste0("mom_medications_ip_", site, "_", date_tag, ".rds"))
  csv_file <- file.path(output_dir, paste0("mom_medications_ip_", site, "_", date_tag, ".csv"))
  
  saveRDS(df, rds_file)
  write_csv(df, csv_file)
  
  message("[", site, "] saved RDS: ", rds_file)
  message("[", site, "] saved CSV: ", csv_file)
  
  return(df)
}
