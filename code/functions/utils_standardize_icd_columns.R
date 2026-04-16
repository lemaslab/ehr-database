standardize_icd_columns <- function(df, site) {
  
  library(dplyr)
  library(rlang)
  
  # ===============================
  # Helper: safe rename
  # ===============================
  rename_if_exists <- function(df, new, old) {
    if (old %in% names(df)) {
      df <- df %>% rename(!!new := !!sym(old))
    }
    return(df)
  }
  
  # ===============================
  # Normalize column names
  # ===============================
  names(df) <- tolower(names(df))
  
  message("[", site, "] standardizing ICD column names")
  
  # ===============================
  # Apply unified mapping
  # ===============================
  df <- df %>%
    rename_if_exists("dx_type", "diagnosis_type") %>%
    rename_if_exists("dx_code", "diagnosis_code") %>%
    rename_if_exists("dx_descrip", "diagnosis_description") %>%
    rename_if_exists("dx_date", "diagnosis_start_date")   # ✅ CRITICAL FIX
  
  # ===============================
  # Debug
  # ===============================
  message("[", site, "] columns after standardization:")
  print(names(df))
  
  return(df)
}