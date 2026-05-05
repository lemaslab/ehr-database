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
# Setup logging
# ===============================
date_tag <- format(Sys.Date(), "%Y%m%d")

log_dir <- file.path(working_dir, "logs")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

log_file <- file.path(log_dir, paste0("mom_icd_", date_tag, ".log"))

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
sink(log_con, type = "message")

run_time <- as.POSIXct(Sys.time(), tz = "America/New_York")
cat("==== MOM ICD RUN ====\n")
cat("Date:", format(run_time, "%Y-%m-%d %H:%M:%S %Z"), "\n\n")

# ===============================
# Load functions
# ===============================
message("=== LOADING ALL FUNCTIONS ===")

func_files <- list.files(
  file.path(working_dir, "code", "functions"),
  pattern = "\\.R$",
  full.names = TRUE
)

lapply(func_files, source)

message("Loaded ", length(func_files), " function files")

library(dplyr)
library(readr)

# ===============================
# Run maternal ICD processing
# ===============================
message("=== RUNNING MOM ICD ===")

gnv_icd <- process_mom_icd(
  site = "GNV",
  working_dir = working_dir
)

jax_icd <- process_mom_icd(
  site = "JAX",
  working_dir = working_dir
)

# ===============================
# Write site-level datasets
# ===============================
message("=== WRITING SITE DATASETS ===")

write_dataset(
  df = gnv_icd,
  dataset_name = "mom_icd_gnv",
  working_dir = working_dir,
  subdir = "GNV"
)

write_dataset(
  df = jax_icd,
  dataset_name = "mom_icd_jax",
  working_dir = working_dir,
  subdir = "JAX"
)

# ===============================
# Combine datasets
# ===============================
message("=== COMBINING DATASETS ===")

mom_icd_all_sites <- bind_rows(gnv_icd, jax_icd) %>%
  distinct()

# ===============================
# ID QA
# ===============================
message("=== ID QA ===")
message("Columns in combined ICD dataset:")
print(names(mom_icd_all_sites))

message("First 10 part_id_mom values:")
print(head(mom_icd_all_sites$part_id_mom, 10))

message("Does deidentified_mom_id remain?")
print("deidentified_mom_id" %in% names(mom_icd_all_sites))

if (!"part_id_mom" %in% names(mom_icd_all_sites)) {
  stop("part_id_mom is missing from mom_icd_all_sites output")
}

if (any(is.na(mom_icd_all_sites$part_id_mom)) || any(mom_icd_all_sites$part_id_mom == "")) {
  warning("Missing or blank part_id_mom values detected")
}

if (!all(grepl("^(AC|DC)-mom-", mom_icd_all_sites$part_id_mom))) {
  warning("Some part_id_mom values do not match expected AC/DC-mom-* format")
}

if ("deidentified_mom_id" %in% names(mom_icd_all_sites)) {
  warning("deidentified_mom_id remains in mom_icd_all_sites")
}

# ===============================
# Write combined dataset
# ===============================
message("=== WRITING COMBINED DATASET ===")
message("Combined rows: ", nrow(mom_icd_all_sites))

write_dataset(
  df = mom_icd_all_sites,
  dataset_name = "mom_icd_all_sites",
  working_dir = working_dir,
  subdir = "COMBINED"
)

# ===============================
# Summary
# ===============================
cat("\n==== SUMMARY ====\n")
cat("Total rows:", nrow(mom_icd_all_sites), "\n")
cat("Unique moms:", dplyr::n_distinct(mom_icd_all_sites$part_id_mom), "\n")

if ("site" %in% names(mom_icd_all_sites)) {
  cat("\nRows by site:\n")
  print(table(mom_icd_all_sites$site))
}

if ("dx_type" %in% names(mom_icd_all_sites)) {
  cat("\nRows by dx_type:\n")
  print(table(mom_icd_all_sites$dx_type, useNA = "ifany"))
}

if ("dx_icd_type" %in% names(mom_icd_all_sites)) {
  cat("\nRows by dx_icd_type:\n")
  print(table(mom_icd_all_sites$dx_icd_type, useNA = "ifany"))
}

# ===============================
# Close log
# ===============================
sink(type = "message")
sink()
close(log_con)

message("=== COMPLETE MOM ICD ===")