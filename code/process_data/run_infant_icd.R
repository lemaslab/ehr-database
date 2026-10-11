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

# Logging is controlled by the master pipeline (or console for standalone runs).
# Never create nested sink() connections inside a stage.
if (!exists("ehr_substage_event", mode="function")) {
  source("code/run/00_pipeline_helpers.R", local=FALSE)
}
substage_name <- "04_infant_icd"
substage_started <- Sys.time()
ehr_substage_event(substage_name,"START",paste("working_dir=",working_dir))
message("[ICD] Starting ",substage_name,"; run log: ",Sys.getenv("EHR_LOG_DIR",unset="<console>"))

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
# Load mom_baby_link latest combined
# ===============================
message("=== LOADING MOM-BABY LINK COMBINED ===")

mom_baby_link <- load_latest_dataset(
  dataset_name = "mom_baby_link_all_sites",
  working_dir = working_dir
)

cat("Loaded mom_baby_link rows:", nrow(mom_baby_link), "\n")

# ===============================
# Split mom-baby link by site
# ===============================
gnv_mb <- mom_baby_link %>% filter(site == "GNV")
jax_mb <- mom_baby_link %>% filter(site == "JAX")

cat("GNV mom_baby_link rows:", nrow(gnv_mb), "\n")
cat("JAX mom_baby_link rows:", nrow(jax_mb), "\n")

# ===============================
# Run infant ICD processing
# ===============================
message("=== RUNNING INFANT ICD ===")

gnv_icd <- process_infant_icd(
  site = "GNV",
  working_dir = working_dir,
  mom_baby_link_df = gnv_mb
)

jax_icd <- process_infant_icd(
  site = "JAX",
  working_dir = working_dir,
  mom_baby_link_df = jax_mb
)

# ===============================
# Write site-level datasets
# ===============================
message("=== WRITING SITE DATASETS ===")

write_dataset(
  df = gnv_icd,
  dataset_name = "infant_icd_gnv",
  working_dir = working_dir,
  subdir = "GNV"
)

write_dataset(
  df = jax_icd,
  dataset_name = "infant_icd_jax",
  working_dir = working_dir,
  subdir = "JAX"
)

# ===============================
# Combine datasets
# ===============================
message("=== COMBINING DATASETS ===")

infant_icd_all_sites <- bind_rows(gnv_icd, jax_icd) %>%
  distinct()

# ===============================
# ID QA
# ===============================
message("=== ID QA ===")
message("Columns in combined infant ICD dataset:")
print(names(infant_icd_all_sites))

message("First 10 part_id_infant values:")
print(head(infant_icd_all_sites$part_id_infant, 10))

message("Does part_id_infant exist?")
print("part_id_infant" %in% names(infant_icd_all_sites))

if (!"part_id_infant" %in% names(infant_icd_all_sites)) {
  stop("part_id_infant is missing from infant_icd_all_sites output")
}

if (any(is.na(infant_icd_all_sites$part_id_infant)) || any(infant_icd_all_sites$part_id_infant == "")) {
  warning("Missing or blank part_id_infant values detected")
}

if (!all(grepl("^(AC|DC)-infant-", infant_icd_all_sites$part_id_infant))) {
  warning("Some part_id_infant values do not match expected AC/DC-infant-* format")
}

if ("part_id_infant_tmp" %in% names(infant_icd_all_sites)) {
  warning("part_id_infant_tmp remains in infant_icd_all_sites")
}

# ===============================
# Write combined dataset
# ===============================
message("=== WRITING COMBINED DATASET ===")
message("Combined rows: ", nrow(infant_icd_all_sites))

write_dataset(
  df = infant_icd_all_sites,
  dataset_name = "infant_icd_all_sites",
  working_dir = working_dir,
  subdir = "COMBINED"
)

# ===============================
# Summary
# ===============================
cat("\n==== SUMMARY ====\n")
cat("Total rows:", nrow(infant_icd_all_sites), "\n")
cat("Unique infants:", dplyr::n_distinct(infant_icd_all_sites$part_id_infant), "\n")

if ("delivery_id" %in% names(infant_icd_all_sites)) {
  cat("Missing delivery_id:", sum(is.na(infant_icd_all_sites$delivery_id)), "\n")
}

if ("site" %in% names(infant_icd_all_sites)) {
  cat("\nRows by site:\n")
  print(table(infant_icd_all_sites$site))
}

if ("dx_category" %in% names(infant_icd_all_sites)) {
  cat("\nRows by dx_category:\n")
  print(table(infant_icd_all_sites$dx_category, useNA = "ifany"))
}

if ("dx_type" %in% names(infant_icd_all_sites)) {
  cat("\nRows by dx_type:\n")
  print(table(infant_icd_all_sites$dx_type, useNA = "ifany"))
}

ehr_substage_event(substage_name,"COMPLETE",
  paste0("rows=",nrow(infant_icd_all_sites),
         "; elapsed_seconds=",round(as.numeric(difftime(Sys.time(),substage_started,units="secs")),1)))
message("=== COMPLETE INFANT ICD ===")
