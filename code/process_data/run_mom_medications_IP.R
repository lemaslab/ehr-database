# ===============================
# Run script for mom IP medications
# ===============================

source("code/functions/process_mom_medications_IP.R")

# ===============================
# Define sites
# ===============================
sites <- c("GNV", "JAX")

# ===============================
# Run processing
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
# Save output
# ===============================
output_dir <- "data/processed"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

output_file <- file.path(output_dir, paste0("mom_medications_ip_all_", format(Sys.Date(), "%Y%m%d"), ".rds"))

saveRDS(mom_medications_ip_all, output_file)

message("Saved to: ", output_file)