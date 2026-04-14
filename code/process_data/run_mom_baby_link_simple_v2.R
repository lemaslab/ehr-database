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
# Load functions
# ===============================
source(file.path(working_dir, "code", "functions", "process_mom_baby_link_simple_v5.R"))

library(dplyr)
library(readr)

# ===============================
# Run processing
# ===============================
message("=== RUNNING MOM BABY LINK (SIMPLE V5) ===")

gnv <- process_mom_baby_link_simple_v5("GNV", working_dir)
jax <- process_mom_baby_link_simple_v5("JAX", working_dir)

# ===============================
# Combine
# ===============================
mom_baby_link <- bind_rows(gnv, jax)

# ===============================
# Output (standardized)
# ===============================
date_tag <- format(Sys.Date(), "%Y%m%d")

out_dir <- file.path(working_dir, "data", "processed", "COMBINED")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

file_base <- paste0("mom_baby_link_all_sites_", date_tag)

save(mom_baby_link, file = file.path(out_dir, paste0(file_base, ".rda")))
write_csv(mom_baby_link, file.path(out_dir, paste0(file_base, ".csv")), na = "")

# ===============================
# Complete
# ===============================
message("=== COMPLETE V5 ===")