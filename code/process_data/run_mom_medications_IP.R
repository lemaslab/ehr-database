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
source("code/functions/process_mom_medications_ip.R")

# -------------------------------
# Define sites
# -------------------------------
sites <- c("GNV", "JAX")

# -------------------------------
# Run processing
# -------------------------------
results <- lapply(sites, function(s) {
  process_mom_medications_ip_v2(s, working_dir)
})

# -------------------------------
# Combine datasets
# -------------------------------
mom_medications_ip_all <- dplyr::bind_rows(results)

message("====================================")
message("FINAL DATASET SUMMARY")
message("====================================")
message("Total rows: ", nrow(mom_medications_ip_all))
message("Sites: ", paste(unique(mom_medications_ip_all$site), collapse = ", "))

# -------------------------------
# Save COMBINED dataset
# -------------------------------
output_dir <- file.path(working_dir, "data", "processed", "COMBINED")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

date_tag <- format(Sys.Date(), "%Y%m%d")

rds_file <- file.path(output_dir, paste0("mom_medications_ip_all_sites_", date_tag, ".rds"))
csv_file <- file.path(output_dir, paste0("mom_medications_ip_all_sites_", date_tag, ".csv"))

saveRDS(mom_medications_ip_all, rds_file)
readr::write_csv(mom_medications_ip_all, csv_file)

message("Saved COMBINED RDS: ", rds_file)
message("Saved COMBINED CSV: ", csv_file)