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
message("Total rows: ", nrow(mom_ip_all_sites))
message("Sites: ", paste(unique(mom_ip_all_sites$site), collapse = ", "))

# -------------------------------
# Write standardized site and combined RDA outputs
# Honors EHR_OUTPUT_MODE, EHR_NETWORK_SYNC, EHR_RELEASE_DATE.
# -------------------------------
source("code/functions/utils_write_dataset.R")
for (i in seq_along(sites)) {
  dataset_name <- paste0("mom_ip_", tolower(sites[[i]]))
  message("[MED] Writing ", dataset_name, " to ", file.path(working_dir,"data","processed",sites[[i]]))
  write_dataset(results[[i]], dataset_name, working_dir=working_dir, subdir=sites[[i]])
}
message("[MED] Writing mom_ip_all_sites to ",file.path(working_dir,"data","processed","COMBINED"))
write_dataset(mom_ip_all_sites, "mom_ip_all_sites", working_dir=working_dir, subdir="COMBINED")
message("[MED] Completed mom_ip; local-only/network settings honored.")
