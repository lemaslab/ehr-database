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
source(file.path(working_dir, "code", "functions", "utils_load_latest_dataset.R"))
source(file.path(working_dir, "code", "functions", "utils_write_dataset.R"))
source(file.path(working_dir, "code", "functions", "process_infant_icd_simple_v2.R"))

library(dplyr)
library(readr)

# ===============================
# Load mom_baby_link (latest combined)
# ===============================
message("=== LOADING MOM-BABY LINK (COMBINED) ===")

mom_baby_link <- load_latest_dataset(
  dataset_name = "mom_baby_link_all_sites",
  working_dir = working_dir
)

cat("Loaded mom_baby_link rows:", nrow(mom_baby_link), "\n")

# ===============================
# Split by site
# ===============================
gnv_mb <- mom_baby_link %>% filter(site == "GNV")
jax_mb <- mom_baby_link %>% filter(site == "JAX")

# ===============================
# Run ICD processing
# ===============================
message("=== RUNNING INFANT ICD ===")

gnv_icd <- process_infant_icd_simple_v2("GNV", working_dir, gnv_mb)
jax_icd <- process_infant_icd_simple_v2("JAX", working_dir, jax_mb)

infant_icd <- bind_rows(gnv_icd, jax_icd)

# ===============================
# Write outputs (standardized)
# ===============================
write_dataset(
  df = infant_icd,
  dataset_name = "infant_icd_all_sites",
  working_dir = working_dir,
  subdir = "COMBINED"
)

# ===============================
# Summary
# ===============================
cat("\n==== SUMMARY ====\n")
cat("Total rows:", nrow(infant_icd), "\n")
cat("Missing delivery_id:", sum(is.na(infant_icd$delivery_id)), "\n")

# ===============================
# Close log
# ===============================
sink(type = "message")
sink()
close(log_con)

message("=== COMPLETE INFANT ICD ===")