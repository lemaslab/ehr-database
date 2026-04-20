# ===============================
# Run script for mom IP medications
# ===============================

source("code/functions/process_mom_medications_IP.R")

# ===============================
# Define sites
# ===============================
sites <- c("GNV", "JAX")

# ===============================
# Process each site
# ===============================
results <- lapply(sites, process_mom_medications_ip)

# ===============================
# Combine datasets
# ===============================
mom_medications_ip_all <- dplyr::bind_rows(results)

message("====================================")
message("FINAL DATASET SUMMARY")
message("====================================")
message("Total rows: ", nrow(mom_medications_ip_all))
message("Sites: ", paste(unique(mom_medications_ip_all$site), collapse = ", "))

# ===============================
# Save output (COMBINED folder)
# ===============================
output_dir <- "data/processed/COMBINED"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

date_tag <- format(Sys.Date(), "%Y%m%d")

output_file <- file.path(
  output_dir,
  paste0("mom_medications_ip_all_sites_", date_tag, ".rds")
)

saveRDS(mom_medications_ip_all, output_file)

message("Saved to: ", output_file)