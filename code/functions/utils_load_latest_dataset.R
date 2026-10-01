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
    stop("No files found for dataset: ", dataset_name, " in ", data_dir)
  }
  
  dates <- gsub(".*_(\\d{8})\\.rda$", "\\1", basename(files))
  dates <- as.Date(dates, format = "%Y%m%d")
  
  latest_file <- files[which.max(dates)]
  
  message("Loading latest ", dataset_name, ": ", basename(latest_file))
  
  tmp_env <- new.env(parent = emptyenv())
  loaded_names <- load(latest_file, envir = tmp_env)
  
  if (length(loaded_names) == 0) {
    stop("No objects found in file: ", latest_file)
  }
  
  if ("mom_baby_link" %in% loaded_names) {
    return(tmp_env$mom_baby_link)
  }
  
  if (dataset_name %in% loaded_names) {
    return(tmp_env[[dataset_name]])
  }
  
  if (length(loaded_names) == 1) {
    return(tmp_env[[loaded_names[1]]])
  }
  
  stop(
    "Loaded file contains multiple objects (",
    paste(loaded_names, collapse = ", "),
    ") and none matched expected names."
  )
}