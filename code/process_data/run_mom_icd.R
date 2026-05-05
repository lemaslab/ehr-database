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

log_file <- file.path(log_dir, paste0("maternal_icd_", date_tag, ".log"))

log_con <- file(log_file, open = "wt")
sink(log_con, split = TRUE)
sink(log_con, type = "message")

run_time <- as.POSIXct(Sys.time(), tz = "America/New_York")
cat("==== MATERNAL ICD RUN (V2) ====\n")
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
message("=== RUNNING MATERNAL ICD V2 ===")

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
  dataset_name = "maternal_icd_gnv",
  working_dir = working_dir,
  subdir = "GNV"
)

write_dataset(
  df = jax_icd,
  dataset_name = "maternal_icd_jax",
  working_dir = working_dir,
  subdir = "JAX"
)

# ===============================
# Combine datasets
# ===============================
message("=== COMBINING DATASETS ===")

maternal_icd <- bind_rows(gnv_icd, jax_icd) %>%
  distinct()

# ===============================
# ID QA
# ===============================
message("=== ID QA ===")
message("Columns in combined ICD dataset:")
print(names(maternal_icd))

message("First 10 part_id_mom values:")
print(head(maternal_icd$part_id_mom, 10))

message("Does deidentified_mom_id remain?")
print("deidentified_mom_id" %in% names(maternal_icd))

if (!"part_id_mom" %in% names(maternal_icd)) {
  stop("part_id_mom is missing from maternal_icd output")
}

if (any(is.na(maternal_icd$part_id_mom)) || any(maternal_icd$part_id_mom == "")) {
  warning("Missing or blank part_id_mom values detected")
}

if (!all(grepl("^(AC|DC)-mom-", maternal_icd$part_id_mom))) {
  warning("Some part_id_mom values do not match expected AC/DC-mom-* format")
}

# ===============================
# Write combined dataset
# ===============================
message("=== WRITING COMBINED DATASET ===")
message("Combined rows: ", nrow(maternal_icd))

write_dataset(
  df = maternal_icd,
  dataset_name = "maternal_icd_all_sites",
  working_dir = working_dir,
  subdir = "COMBINED"
)

# ===============================
# Summary
# ===============================
cat("\n==== SUMMARY ====\n")
cat("Total rows:", nrow(maternal_icd), "\n")
cat("Unique moms:", dplyr::n_distinct(maternal_icd$part_id_mom), "\n")

if ("site" %in% names(maternal_icd)) {
  cat("\nRows by site:\n")
  print(table(maternal_icd$site))
}

if ("dx_category" %in% names(maternal_icd)) {
  cat("\nRows by dx_category:\n")
  print(table(maternal_icd$dx_category, useNA = "ifany"))
}

# ===============================
# Close log
# ===============================
sink(type = "message")
sink()
close(log_con)

message("=== COMPLETE MATERNAL ICD V2 ===")