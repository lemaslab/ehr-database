write_dataset <- function(df,
                          dataset_name,
                          working_dir = getwd(),
                          subdir = "COMBINED",
                          write_network = TRUE,
                          network_base = "V:/FACULTY/DJLEMAS/EHR_Data_processed") {
  suppressPackageStartupMessages({
    library(readr)
  })

  date_tag <- format(Sys.Date(), "%Y%m%d")

  local_dir <- file.path(
    working_dir,
    "data", "processed",
    subdir
  )

  dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)

  file_base <- paste0(dataset_name, "_", date_tag)

  rda_path <- file.path(local_dir, paste0(file_base, ".rda"))
  csv_path <- file.path(local_dir, paste0(file_base, ".csv"))

  save(df, file = rda_path)
  write_csv(df, csv_path, na = "")

  message("Saved locally: ", basename(rda_path))

  if (write_network && dir.exists(network_base)) {
    network_dir <- file.path(network_base, subdir)
    dir.create(network_dir, recursive = TRUE, showWarnings = FALSE)

    save(df, file = file.path(network_dir, paste0(file_base, ".rda")))
    write_csv(df, file.path(network_dir, paste0(file_base, ".csv")), na = "")

    message("Saved to network: ", network_dir)
  } else if (write_network) {
    warning("Network path not available — skipped network write")
  }

  return(list(rda = rda_path, csv = csv_path))
}
