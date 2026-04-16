clean_icd <- function(df, id_col, type_col, code_col, desc_col, date_col) {
  
  library(dplyr)
  library(lubridate)
  library(rlang)
  
  message("=== RUNNING clean_icd() ===")
  
  df <- df %>%
    rename(
      part_id_mom = !!sym(id_col),
      dx_type     = !!sym(type_col),
      dx_code     = !!sym(code_col),
      dx_descrip  = !!sym(desc_col),
      dx_date     = !!sym(date_col)
    ) %>%
    
    mutate(
      dx_code = as.character(dx_code),
      dx_type = as.character(dx_type),
      
      # 🔥 FIX: robust date parsing
      dx_date = suppressWarnings(parse_date_time(
        dx_date,
        orders = c("ymd", "mdy", "dmy")
      ))
    ) %>%
    
    filter(
      !is.na(part_id_mom),
      !is.na(dx_code)
    ) %>%
    
    distinct()
  
  message("clean_icd(): rows after cleaning = ", nrow(df))
  message("clean_icd(): missing dx_date = ", sum(is.na(df$dx_date)))
  
  return(df)
}