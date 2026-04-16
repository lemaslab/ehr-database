standardize_icd_columns <- function(df, site) {
  
  # ===============================
  # JAX mapping
  # ===============================
  if (site == "JAX") {
    
    message("[JAX] standardizing ICD column names")
    
    df <- df %>%
      rename(
        dx_type    = diagnosis_type,
        dx_code    = diagnosis_code,
        dx_descrip = diagnosis_description,
        dx_date    = diagnosis_date
      )
  }
  
  # ===============================
  # GNV (optional safety check)
  # ===============================
  if (site == "GNV") {
    
    # only rename if needed (prevents breaking working pipeline)
    if (!all(c("dx_type","dx_code","dx_descrip","dx_date") %in% names(df))) {
      
      message("[GNV] standardizing ICD column names (fallback)")
      
      df <- df %>%
        rename(
          dx_type    = diagnosis_type,
          dx_code    = diagnosis_code,
          dx_descrip = diagnosis_description,
          dx_date    = diagnosis_date
        )
    }
  }
  
  return(df)
}