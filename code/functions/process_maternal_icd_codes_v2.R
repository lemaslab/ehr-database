process_mom_medications_ip_v2 <- function(site, working_dir) {
  
  message("=== PROCESSING MOM MEDICATIONS IP V2: ", site, " ===")
  
  library(dplyr)
  library(readr)
  library(lubridate)
  library(stringr)
  
  # ===============================
  # Site mapping (MATCH ICD)
  # ===============================
  map_site_name <- function(site) {
    if (site == "GNV") return("AC-mom-1")
    if (site == "JAX") return("DC-mom-1")
    return(site)
  }
  
  site_std <- map_site_name(site)
  
  # ===============================
  # Define file paths (KEEP YOURS)
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
  exists_flag <- file.exists(file)
  message("[", site, "] ", file, " | exists=", exists_flag)
  
  if (!exists_flag) {
    stop("[", site, "] Missing file: ", file)
  }
  
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
  # Harmonize variables (KEEP STRUCTURE)
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id = as.character(deidentified_mom_id),
      
      med_name = coalesce(
        `med order display name`,
        med_name
      ),
      
      med_code = coalesce(
        `rxnorm code`,
        med_code
      ),
      
      start_date = coalesce(
        `taken datetime`,
        start_date
      ),
      
      med_name = as.character(med_name),
      med_code = as.character(med_code),
      start_date = parse_date_time(
        start_date,
        orders = c("ymd", "mdy", "dmy", "Ymd"),
        quiet = TRUE
      )
    )
  
  # ===============================
  # Cleanup + STANDARDIZED ID (MATCH ICD)
  # ===============================
  df <- df %>%
    mutate(
      deidentified_mom_id = trimws(deidentified_mom_id),
      med_name = str_to_upper(trimws(med_name)),
      med_code = trimws(med_code),
      start_date = as.Date(start_date),
      
      site = site_std,
      
      # 🔥 SAME STRUCTURE AS ICD
      part_id_mom = paste0(site_std, "-", deidentified_mom_id)
    ) %>%
    filter(
      !is.na(deidentified_mom_id),
      deidentified_mom_id != "",
      !is.na(med_name),
      med_name != ""
    ) %>%
    distinct() %>%
    
    # ===============================
  # FINAL STRUCTURE (ICD-STYLE)
  # ===============================
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
  # EXPORT (SITE-SPECIFIC, MATCH ICD)
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