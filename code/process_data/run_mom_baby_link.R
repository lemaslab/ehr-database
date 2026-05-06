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
# Setup logging (FIXED)
# ===============================
date_tag <- format(Sys.Date(), "%Y%m%d")

log_dir <- file.path(working_dir, "logs")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

log_file <- file.path(log_dir, paste0("mom_baby_link_", date_tag, ".log"))

# open connection
log_con <- file(log_file, open = "wt")

# redirect output + messages
sink(log_con, split = TRUE)
sink(log_con, type = "message")

cat("==== MOM BABY LINK RUN ====\n")
run_time <- as.POSIXct(Sys.time(), tz = "America/New_York")
cat("Date:", format(run_time, "%Y-%m-%d %H:%M:%S %Z"), "\n\n")

# ===============================
# Load functions
# ===============================
source(file.path(working_dir, "code", "functions", "process_mom_baby_link.R"))

library(dplyr)
library(readr)

# ===============================
# Run processing
# ===============================
message("=== RUNNING MOM BABY LINK (SIMPLE V5) ===")

gnv <- process_mom_baby_link("GNV", working_dir)
jax <- process_mom_baby_link("JAX", working_dir)

# ===============================
# Combine
# ===============================
mom_baby_link_all_sites <- bind_rows(gnv, jax)

# ===============================
# Output paths
# ===============================
local_out_dir <- file.path(working_dir, "data", "processed", "COMBINED")
network_base <- "V:/FACULTY/DJLEMAS/EHR_Data_processed"

network_out_dir <- file.path(network_base, "COMBINED")

dir.create(local_out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(network_out_dir, recursive = TRUE, showWarnings = FALSE)

file_base <- paste0("mom_baby_link_all_sites_", date_tag)

# ===============================
# Write outputs
# ===============================
# Local
save(mom_baby_link_all_sites, file = file.path(local_out_dir, paste0(file_base, ".rda")))
write_csv(mom_baby_link_all_sites, file.path(local_out_dir, paste0(file_base, ".csv")), na = "")

# Network
if (dir.exists(network_base)) {
  save(mom_baby_link, file = file.path(network_out_dir, paste0(file_base, ".rda")))
  write_csv(mom_baby_link, file.path(network_out_dir, paste0(file_base, ".csv")), na = "")
  message("Network write successful")
} else {
  warning("Network path not available — skipped network write")
}

# ===============================
# Summary
# ===============================
cat("\n==== SUMMARY ====\n")
cat("Rows:", nrow(mom_baby_link_all_sites), "\n")
cat("Missing DOB:", sum(is.na(mom_baby_link_all_sites$part_dob_infant)), "\n")

# ===============================
# Close log
# ===============================
sink(type = "message")
sink()

message("=== COMPLETE V5 ===")