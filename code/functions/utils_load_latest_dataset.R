load_latest_dataset <- function(dataset_name,
                                working_dir = getwd(),
                                subdir = "COMBINED") {
  
  data_dir <- file.path(
    working_dir,
    "data", "processed",
    subdir
  )
  
  if (!dir.exists(data_dir)) {
    stop("Directory does not exist: ", data_dir)
  }
  
  pattern <- paste0("^", dataset_name, "_[0-9]{8}\\.rda$")
  
  files <- list.files(data_dir, pattern = pattern, full.names = TRUE)
  
  if (length(files) == 0) {
    stop("No files found for dataset: ", dataset_name)
  }
  
  dates <- gsub(".*_(\\d{8})\\.rda$", "\\1", basename(files))
  dates <- as.Date(dates, format = "%Y%m%d")
  
  latest_file <- files[which.max(dates)]
  
  message("Loading latest ", dataset_name, ": ", basename(latest_file))
  
  load(latest_file)
  
  obj_name <- ls()[length(ls())]
  
  return(get(obj_name))
}
