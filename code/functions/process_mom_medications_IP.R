process_mom_medications_IP <- function(site, working_dir) {
  
  message("=== PROCESSING MOM MEDICATIONS (IP): ", site, " ===")
  
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(stringr)
  })
  
  # -----------------------------
  # Define file paths
  # -----------------------------
  site_paths <- list(
    GNV = file.path(working_dir, "data", "gnv_only", "mom_medications_IP.csv"),
    JAX = "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/mom_IP_Jax.csv"
  )
  
  if (!site %in% names(site_paths)) {
    stop("Unknown site: ", site)
  }
  
  mom_file <- site_paths[[site]]
  
  if (!file.exists(mom_file)) {
    stop("[", site, "] file not found: ", mom_file)
  }
  
  message("[", site, "] reading: ", mom_file)
  
  df <- read_csv(mom_file, show_col_types = FALSE)
  
  message("[", site, "] rows: ", nrow(df))
  message("[", site, "] columns: ", paste(names(df), collapse = ", "))
  
  # -----------------------------
  # Normalize column names
  # -----------------------------
  names(df) <- tolower(names(df))
  names(df) <- gsub("_jax$", "", names(df))
  
  # -----------------------------
  # Add site flag
  # -----------------------------
  df <- df %>%
    mutate(site = site)
  
  # -----------------------------
  # OPTIONAL: Harmonize key variables
  # (edit once you inspect JAX vs GNV)
  # -----------------------------
  # Example pattern:
  # df <- df %>%
  #   rename(
  #     deidentified_mom_id = patient_id,
  #     medication_name = med_name
  #   )
  
  # -----------------------------
  # Medication classification
  # -----------------------------
  if ("medication_name" %in% names(df)) {
    
    df <- df %>%
      mutate(
        med_class = case_when(
          str_detect(medication_name, regex("metformin|insulin", ignore_case = TRUE)) ~ "diabetes",
          str_detect(medication_name, regex("lisinopril|amlodipine|labetalol", ignore_case = TRUE)) ~ "hypertension",
          str_detect(medication_name, regex("semaglutide|liraglutide|dulaglutide", ignore_case = TRUE)) ~ "glp1",
          str_detect(medication_name, regex("phentermine", ignore_case = TRUE)) ~ "aom",
          TRUE ~ "other"
        )
      )
    
  } else {
    message("[", site, "] WARNING: medication_name column not found")
  }
  
  # -----------------------------
  # Basic QC
  # -----------------------------
  message("[", site, "] completed processing")
  
  return(df)
}