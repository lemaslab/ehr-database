write_dataset <- function(df,
                          dataset_name,
                          working_dir = getwd(),
                          subdir = "COMBINED",
                          write_network = TRUE,
                          network_base = "V:/FACULTY/DJLEMAS/EHR_Data_processed") {
  
  suppressPackageStartupMessages({
    library(readr)
  })
  
  # ===============================
  # Date tag
  # ===============================
  date_tag <- format(Sys.Date(), "%Y%m%d")
  
  # ===============================
  # Local directory
  # ===============================
  local_dir <- file.path(
    working_dir,
    "data", "processed",
    subdir
  )
  
  dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)
  
  # ===============================
  # File naming
  # ===============================
  file_base <- paste0(dataset_name, "_", date_tag)
  
  rda_path <- file.path(local_dir, paste0(file_base, ".rda"))
  csv_path <- file.path(local_dir, paste0(file_base, ".csv"))
  
  # ===============================
  # Save locally (FIXED OBJECT NAME)
  # ===============================
  assign(dataset_name, df)
  save(list = dataset_name, file = rda_path)
  rm(list = dataset_name)
  
  write_csv(df, csv_path, na = "")
  
  message("Saved locally: ", basename(rda_path))
  
  # ===============================
  # Optional network write
  # ===============================
  if (write_network && dir.exists(network_base)) {
    
    network_dir <- file.path(network_base, subdir)
    dir.create(network_dir, recursive = TRUE, showWarnings = FALSE)
    
    assign(dataset_name, df)
    save(list = dataset_name, file = file.path(network_dir, paste0(file_base, ".rda")))
    rm(list = dataset_name)
    
    write_csv(df, file.path(network_dir, paste0(file_base, ".csv")), na = "")
    
    message("Saved to network: ", network_dir)
    
  } else if (write_network) {
    warning("Network path not available — skipped network write")
  }
  
  # ===============================
  # Return paths
  # ===============================
  return(list(
    rda = rda_path,
    csv = csv_path
  ))
}