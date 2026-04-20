process_mom_medications_ip_v2 <- function(site, working_dir) {
  
  message("=== PROCESSING MOM MEDICATIONS IP V2: ", site, " ===")
  
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
    library(stringr)
  })
  
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
  message("[", site, "] columns:")
  print(names(df))
  
  # ===============================
  # Standardize column names
  # ===============================
  names(df) <- tolower(names(df))
  
  # ===============================
  # Helper: safe column picker
  # ===============================
  pick_col <- function(df, candidates) {
    existing <- intersect(candidates, names(df))
    if (length(existing) == 0) return(NULL)
    return(existing[1])
  }
  
  mom_id_col   <- pick_col(df, c("deidentified_mom_id", "mom_id"))
  med_name_col <- pick_col(df, c("med order display name", "med_order_display_name", "med_name"))
  med_code_col <- pick_col(df, c("rxnorm code", "rxnorm_code", "med_code"))
  date_col     <- pick_col(df, c("taken datetime", "taken_datetime", "start_date"))
  
  # ===============================
  # Harmonize variables
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id = if (!is.null(mom_id_col)) as.character(.data[[mom_id_col]]) else NA_character_,
      med_name            = if (!is.null(med_name_col)) as.character(.data[[med_name_col]]) else NA_character_,
      med_code            = if (!is.null(med_code_col)) as.character(.data[[med_code_col]]) else NA_character_,
      start_date          = if (!is.null(date_col)) .data[[date_col]] else NA
    ) %>%
    mutate(
      start_date = parse_date_time(
        start_date,
        orders = c("ymd", "mdy", "dmy", "Ymd"),
        quiet = TRUE
      )
    )
  
  # ===============================
  # Cleanup + STANDARDIZED ID
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id = trimws(deidentified_mom_id),
      med_name            = str_to_upper(trimws(med_name)),
      med_code            = trimws(med_code),
      start_date          = as.Date(start_date),
      
      # keep raw site
      site = site,
      
      # match ICD script
      part_id_mom = case_when(
        site == "GNV" ~ paste0("AC-mom-", deidentified_mom_id),
        site == "JAX" ~ paste0("DC-mom-", deidentified_mom_id),
        TRUE ~ deidentified_mom_id
      )
    ) %>%
    filter(
      !is.na(deidentified_mom_id),
      deidentified_mom_id != "",
      !is.na(med_name),
      med_name != ""
    ) %>%
    distinct() %>%
    select(
      part_id_mom,
      start_date,
      med_name,
      med_code,
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