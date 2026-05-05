# ===============================
# Run script for mom IP medications
# ===============================

# -------------------------------
# Locate repo root
# -------------------------------
possible_roots <- c(
  getwd(),
  "C:/Users/djlemas/Documents/GitHub/ehr-database",
  "C:/Users/djlemas/OneDrive/Documents/ehr-database"
)

working_dir <- NULL
for (p in possible_roots) {
  if (file.exists(file.path(p, "code"))) {
    working_dir <- normalizePath(p)
    break
  }
}

if (is.null(working_dir)) {
  stop("Could not locate repo root")
}

setwd(working_dir)

message("Using working_dir: ", working_dir)

# -------------------------------
# Source processing function
# -------------------------------
source("code/functions/process_mom_ip.R")

# -------------------------------
# Define sites
# -------------------------------
sites <- c("GNV", "JAX")

# -------------------------------
# Run processing
# -------------------------------
results <- lapply(sites, function(s) {
  process_mom_ip(s, working_dir)
})

# -------------------------------
# Combine datasets
# -------------------------------
mom_ip_all_sites <- dplyr::bind_rows(results)

message("====================================")
message("FINAL DATASET SUMMARY")
message("====================================")
message("Total rows: ", nrow(mom_medications_ip_all))
message("Sites: ", paste(unique(mom_medications_ip_all$site), collapse = ", "))

# -------------------------------
# Save COMBINED dataset
# -------------------------------

# Local repo output
local_output_dir <- file.path(working_dir, "data", "processed", "COMBINED")
dir.create(local_output_dir, recursive = TRUE, showWarnings = FALSE)

# Shared drive output
shared_output_dir <- "V:/FACULTY/DJLEMAS/EHR_Data_processed/COMBINED"
dir.create(shared_output_dir, recursive = TRUE, showWarnings = FALSE)

date_tag <- format(Sys.Date(), "%Y%m%d")

# Object/file name convention
rda_file_local <- file.path(local_output_dir, paste0("mom_ip_all_sites_", date_tag, ".rda"))
csv_file_local <- file.path(local_output_dir, paste0("mom_ip_all_sites_", date_tag, ".csv"))

rda_file_shared <- file.path(shared_output_dir, paste0("mom_ip_all_sites_", date_tag, ".rda"))
csv_file_shared <- file.path(shared_output_dir, paste0("mom_ip_all_sites_", date_tag, ".csv"))

# Save local
save(mom_ip_all_sites, file = rda_file_local)
readr::write_csv(mom_ip_all_sites, csv_file_local)

# Save shared drive
save(mom_ip_all_sites, file = rda_file_shared)
readr::write_csv(mom_ip_all_sites, csv_file_shared)

message("Saved LOCAL RDA: ", rda_file_local)
message("Saved LOCAL CSV: ", csv_file_local)
message("Saved SHARED RDA: ", rda_file_shared)
message("Saved SHARED CSV: ", csv_file_shared)
