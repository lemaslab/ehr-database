# ===============================
# Load libraries
# ===============================
suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
})

# ===============================
# Locate repo root
# ===============================
possible_roots <- c(
  getwd(),
  "C:/Users/djlemas/Documents/GitHub/ehr-database",
  "C:/Users/djlemas/OneDrive/Documents/ehr-database"
)

working_dir <- NULL
for (p in possible_roots) {
  if (file.exists(file.path(p, "code", "functions"))) {
    working_dir <- normalizePath(p)
    break
  }
}

if (is.null(working_dir)) {
  stop("Could not locate repo root")
}

message("Using working_dir: ", working_dir)

# ===============================
# Define data paths
# ===============================
get_mom_ip_med_path <- function(site) {
  
  if (site == "GNV") {
    return("V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/mom_medication/mom_IP.csv")
  }
  
  if (site == "JAX") {
    return("V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/mom_IP_Jax.csv")
  }
  
  stop(paste("Unknown site:", site))
}

# ===============================
# Read data
# ===============================
read_mom_ip_data <- function(site) {
  
  path <- get_mom_ip_med_path(site)
  
  if (!file.exists(path)) {
    stop("File does not exist: ", path)
  }
  
  message("[", site, "] reading: ", basename(path))
  
  df <- readr::read_csv(path, show_col_types = FALSE)
  
  message("[", site, "] rows: ", nrow(df))
  message("[", site, "] columns: ", paste(names(df), collapse = ", "))
  
  return(df)
}

# ===============================
# Harmonize columns across sites
# ===============================
harmonize_mom_ip_medications <- function(df, site) {
  
  names(df) <- tolower(names(df))
  
  df <- df %>%
    rename(
      mom_id        = dplyr::coalesce(
        !!!rlang::syms(intersect(c("mom_id", "deidentified_mom_id", "mother_id"), names(df)))
      ),
      encounter_id  = dplyr::coalesce(
        !!!rlang::syms(intersect(c("encounter_id", "visit_id"), names(df)))
      ),
      med_name      = dplyr::coalesce(
        !!!rlang::syms(intersect(c("med_name", "medication_name", "drug_name"), names(df)))
      ),
      med_code      = dplyr::coalesce(
        !!!rlang::syms(intersect(c("med_code", "ndc", "rxnorm_code"), names(df)))
      ),
      start_date    = dplyr::coalesce(
        !!!rlang::syms(intersect(c("start_date", "med_start_date", "order_date"), names(df)))
      ),
      end_date      = dplyr::coalesce(
        !!!rlang::syms(intersect(c("end_date", "med_end_date", "stop_date"), names(df)))
      )
    )
  
  df <- df %>%
    mutate(
      site = site,
      med_name = str_to_upper(med_name)
    )
  
  return(df)
}

# ===============================
# Main processing function
# ===============================
process_mom_medications_ip <- function(site) {
  
  message("====================================")
  message("PROCESSING MOM MEDICATIONS IP:", site)
  message("====================================")
  
  df <- read_mom_ip_data(site)
  
  df <- harmonize_mom_ip_medications(df, site)
  
  message("[", site, "] after harmonization rows: ", nrow(df))
  
  return(df)
}