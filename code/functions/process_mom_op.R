process_mom_op <- function(site, working_dir) {
  
  message("=== PROCESSING MOM MEDICATIONS OP V2: ", site, " ===")
  
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
    file <- file.path(base_root, "2021/dataset_09_2021/mom_medication/mom_OP.csv")
  } else if (site == "JAX") {
    file <- file.path(base_root, "2025/dataset_04_2025/med_OP_Jax.csv")
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
  # Expected raw columns:
  # Deidentified_mom_ID, Med Order Display Name,
  # Medication History Category, Med Order Datetime
  # or Deid-Med Order Datetime,
  # Med Therapy Class, Pharmacy Class,
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
  
  med_hx_cat_col <- pick_col(
    df,
    c("medication_history_category", "med_hx_cat", "med_history_category"),
    required = FALSE,
    label = "medication history category column"
  )
  
  date_med_order_col <- pick_col(
    df,
    c(
      "deid_med_order_datetime",
      "med_order_datetime",
      "date_med_order",
      "order_datetime",
      "date_med_taken",
      "taken_datetime",
      "deid_taken_datetime",
      "taken_date",
      "start_date",
      "med_start_date"
    ),
    required = FALSE,
    label = "medication order datetime column"
  )
  
  med_tx_class_col <- pick_col(
    df,
    c("med_therapy_class", "med_tx_class", "medication_therapy_class"),
    required = FALSE,
    label = "med therapy class column"
  )
  
  pharm_class_col <- pick_col(
    df,
    c("pharmacy_class", "pharm_class"),
    required = FALSE,
    label = "pharmacy class column"
  )
  
  pharm_subclass_col <- pick_col(
    df,
    c("pharmacy_subclass", "pharm_subclass"),
    required = FALSE,
    label = "pharmacy subclass column"
  )
  
  rxnorm_cd_col <- pick_col(
    df,
    c("rxnorm_code", "rxnorm_cd", "rxnorm", "med_code"),
    required = FALSE,
    label = "rxnorm code column"
  )
  
  message("[", site, "] selected columns:")
  message("  mom_id_col: ", mom_id_col)
  message("  med_name_col: ", med_name_col)
  message("  med_hx_cat_col: ", ifelse(is.null(med_hx_cat_col), "NULL", med_hx_cat_col))
  message("  date_med_order_col: ", ifelse(is.null(date_med_order_col), "NULL", date_med_order_col))
  message("  med_tx_class_col: ", ifelse(is.null(med_tx_class_col), "NULL", med_tx_class_col))
  message("  pharm_class_col: ", ifelse(is.null(pharm_class_col), "NULL", pharm_class_col))
  message("  pharm_subclass_col: ", ifelse(is.null(pharm_subclass_col), "NULL", pharm_subclass_col))
  message("  rxnorm_cd_col: ", ifelse(is.null(rxnorm_cd_col), "NULL", rxnorm_cd_col))
  
  # ===============================
  # Harmonize variables
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id = as.character(.data[[mom_id_col]]),
      med_name            = as.character(.data[[med_name_col]]),
      med_hx_cat          = if (!is.null(med_hx_cat_col)) as.character(.data[[med_hx_cat_col]]) else NA_character_,
      date_med_order_raw  = if (!is.null(date_med_order_col)) as.character(.data[[date_med_order_col]]) else NA_character_,
      med_tx_class        = if (!is.null(med_tx_class_col)) as.character(.data[[med_tx_class_col]]) else NA_character_,
      pharm_class         = if (!is.null(pharm_class_col)) as.character(.data[[pharm_class_col]]) else NA_character_,
      pharm_subclass      = if (!is.null(pharm_subclass_col)) as.character(.data[[pharm_subclass_col]]) else NA_character_,
      rxnorm_cd           = if (!is.null(rxnorm_cd_col)) as.character(.data[[rxnorm_cd_col]]) else NA_character_
    ) %>%
    mutate(
      date_med_order = parse_date_time(
        date_med_order_raw,
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
    )
  
  # ===============================
  # Cleanup
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id = trimws(deidentified_mom_id),
      med_name            = str_to_upper(trimws(med_name)),
      med_hx_cat          = trimws(med_hx_cat),
      med_tx_class        = trimws(med_tx_class),
      pharm_class         = trimws(pharm_class),
      pharm_subclass      = trimws(pharm_subclass),
      rxnorm_cd           = trimws(rxnorm_cd),
      site                = site
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
      med_name,
      med_hx_cat,
      date_med_order,
      date_med_order_raw,
      med_tx_class,
      pharm_class,
      pharm_subclass,
      rxnorm_cd,
      site
    )
  
  message("[", site, "] rows after cleanup: ", nrow(df))
  message("[", site, "] unique moms: ", dplyr::n_distinct(df$part_id_mom))
  message("[", site, "] missing date_med_order: ", sum(is.na(df$date_med_order)))
  
  # ===============================
  # Return processed site-level dataset
  # Export is handled by run_mom_medications_OP.R
  # ===============================
  
  return(df)
}
