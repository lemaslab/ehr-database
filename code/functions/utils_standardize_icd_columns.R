standardize_icd_columns <- function(df, site) {
  
  library(dplyr)
  library(rlang)
  
  # ===============================
  # Normalize column names
  # ===============================
  names(df) <- tolower(names(df))
  
  # 🔥 CRITICAL FIX: replace spaces with underscores
  names(df) <- gsub(" ", "_", names(df))
  
  message("[", site, "] standardizing ICD column names")
  
  # ===============================
  # Safe rename helper
  # ===============================
  rename_if_exists <- function(df, new, old) {
    if (old %in% names(df)) {
      df <- df %>% rename(!!new := !!sym(old))
    }
    return(df)
  }
  
  # ===============================
  # Apply mapping
  # ===============================
  df <- df %>%
    rename_if_exists("dx_type", "diagnosis_type") %>%
    rename_if_exists("dx_code", "diagnosis_code") %>%
    rename_if_exists("dx_descrip", "diagnosis_description") %>%
    rename_if_exists("dx_date", "diagnosis_start_date")
  
  # ===============================
  # Debug output
  # ===============================
  message("[", site, "] columns after standardization:")
  print(names(df))
  
  return(df)
}