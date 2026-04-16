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
cat("==== MATERNAL ICD RUN ====\n")
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
# Load mom_baby_link
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

cat("GNV rows:", nrow(gnv_mb), "\n")
cat("JAX rows:", nrow(jax_mb), "\n")

# ===============================
# Run maternal ICD processing
# ===============================
message("=== RUNNING MATERNAL ICD ===")

gnv_icd <- process_maternal_icd_codes_v1(
  site = "GNV",
  working_dir = working_dir,
  mom_baby_link_df = gnv_mb
)

jax_icd <- process_maternal_icd_codes_v1(
  site = "JAX",
  working_dir = working_dir,
  mom_baby_link_df = jax_mb
)

# ===============================
# ✅ INSERT HERE (SITE-LEVEL WRITES)
# ===============================
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
maternal_icd <- bind_rows(gnv_icd, jax_icd) %>%
  distinct()

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

message("=== COMPLETE MATERNAL ICD ===")