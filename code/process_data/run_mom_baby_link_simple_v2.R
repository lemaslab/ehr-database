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

source(file.path(working_dir, "code", "functions", "process_mom_baby_link_simple_v2.R"))

library(dplyr)

message("=== RUNNING MOM BABY LINK (SIMPLE V2) ===")

gnv <- process_mom_baby_link_simple_v2("GNV", working_dir)
jax <- process_mom_baby_link_simple_v2("JAX", working_dir)

combined <- bind_rows(gnv, jax)

out_dir <- file.path(working_dir, "data", "processed", "COMBINED")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

save(combined, file = file.path(out_dir, "mom_baby_link_all_sites.rda"))
readr::write_csv(combined, file.path(out_dir, "mom_baby_link_all_sites.csv"), na = "")

message("=== COMPLETE V2 ===")
