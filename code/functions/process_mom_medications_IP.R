# ===============================
# Load libraries
# ===============================
suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(tibble)
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
# Map site names
# ===============================
map_site_name <- function(site) {
  if (site == "GNV") return("AC-mom-1")
  if (site == "JAX") return("DC-mom-1")
  return(site)
}

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
  
  df <- read_csv(path, show_col_types = FALSE, progress = FALSE)
  
  message("[", site, "] rows: ", nrow(df))
  message("[", site, "] columns: ", paste(names(df), collapse = ", "))
  
  return(df)
}

# ===============================
# Helper: pick first existing column
# ===============================
pick_first_existing <- function(df, candidates) {
  existing <- intersect(candidates, names(df))
  if (length(existing) == 0) return(NULL)
  return(existing[1])
}

# ===============================
# Harmonize columns
# ===============================
harmonize_mom_ip_medications <- function(df, site) {
  
  names(df) <- tolower(names(df))
  
  mom_id_col <- pick_first_existing(df, c("mom_id", "deidentified_mom_id", "mother_id"))
  encounter_col <- pick_first_existing(df, c("encounter_id", "visit_id"))
  med_name_col <- pick_first_existing(df, c("med_name", "medication_name", "med order display name", "med_order_display_name", "drug_name"))
  med_code_col <- pick_first_existing(df, c("med_code", "ndc", "rxnorm code", "rxnorm_code"))
  start_col <- pick_first_existing(df, c("start_date", "med_start_date", "order_date", "taken datetime", "taken_datetime"))
  end_col <- pick_first_existing(df, c("end_date", "med_end_date", "stop_date"))
  
  df_std <- tibble(
    mom_id       = if (!is.null(mom_id_col)) df[[mom_id_col]] else NA,
    encounter_id = if (!is.null(encounter_col)) df[[encounter_col]] else NA,
    med_name     = if (!is.null(med_name_col)) df[[med_name_col]] else NA,
    med_code     = if (!is.null(med_code_col)) df[[med_code_col]] else NA,
    start_date   = if (!is.null(start_col)) df[[start_col]] else NA,
    end_date     = if (!is.null(end_col)) df[[end_col]] else NA,
    site         = map_site_name(site)
  ) %>%
    mutate(
      med_name = str_to_upper(as.character(med_name))
    )
  
  return(df_std)
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
  
  message("[", site, "] standardized rows: ", nrow(df))
  
  return(df)
}