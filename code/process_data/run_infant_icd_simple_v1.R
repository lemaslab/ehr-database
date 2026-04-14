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

if (is.null(working_dir)) stop("Could not locate repo root")

message("Using working_dir: ", working_dir)

# ===============================
# Setup logging
# ===============================
date_tag <- format(Sys.Date(), "%Y%m%d")

log_dir <- file.path(working_dir, "logs")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

log_file <- file.path(log_dir, paste0("infant_icd_", date_tag, ".log"))

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
sink(log_con, type = "message")

run_time <- as.POSIXct(Sys.time(), tz = "America/New_York")
cat("==== INFANT ICD RUN ====\n")
cat("Date:", format(run_time, "%Y-%m-%d %H:%M:%S %Z"), "\n\n")

# ===============================
# Load functions
# ===============================
source(file.path(working_dir, "code", "functions", "process_mom_baby_link_simple_v5.R"))
source(file.path(working_dir, "code", "functions", "process_infant_icd_simple_v2.R"))

library(dplyr)
library(readr)

# ===============================
# Build mom-baby link
# ===============================
message("=== BUILDING MOM-BABY LINK ===")

gnv_mb <- process_mom_baby_link_simple_v5("GNV", working_dir)
jax_mb <- process_mom_baby_link_simple_v5("JAX", working_dir)

# ===============================
# Run ICD
# ===============================
message("=== RUNNING INFANT ICD ===")

gnv_icd <- process_infant_icd_simple_v2("GNV", working_dir, gnv_mb)
jax_icd <- process_infant_icd_simple_v2("JAX", working_dir, jax_mb)

infant_icd <- bind_rows(gnv_icd, jax_icd)

# ===============================
# Output combined
# ===============================
local_dir <- file.path(working_dir, "data", "processed", "COMBINED")
network_base <- "V:/FACULTY/DJLEMAS/EHR_Data_processed"
network_dir <- file.path(network_base, "COMBINED")

dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(network_dir, recursive = TRUE, showWarnings = FALSE)

file_base <- paste0("infant_icd_all_sites_", date_tag)

save(infant_icd, file = file.path(local_dir, paste0(file_base, ".rda")))
write_csv(infant_icd, file.path(local_dir, paste0(file_base, ".csv")), na = "")

if (dir.exists(network_base)) {
  save(infant_icd, file = file.path(network_dir, paste0(file_base, ".rda")))
  write_csv(infant_icd, file.path(network_dir, paste0(file_base, ".csv")), na = "")
  message("Network write successful")
} else {
  warning("Network path not available — skipped network write")
}

# ===============================
# Summary
# ===============================
cat("\n==== SUMMARY ====\n")
cat("Rows:", nrow(infant_icd), "\n")

# ===============================
# Close log
# ===============================
sink(type = "message")
sink()
close(log_con)

message("=== COMPLETE INFANT ICD ===")