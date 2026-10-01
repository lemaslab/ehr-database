# Stage 01: canonical mother-infant-delivery linkage

source("code/run/00_pipeline_helpers.R")
source("code/functions/process_mom_baby_link.R")

suppressPackageStartupMessages(library(dplyr))

working_dir <- getwd()

gnv <- load_or_build(
  "GNV", "mom_baby_link",
  function() process_mom_baby_link("GNV", working_dir),
  working_dir
)

jax <- load_or_build(
  "JAX", "mom_baby_link",
  function() process_mom_baby_link("JAX", working_dir),
  working_dir
)

mom_baby_link_all_sites <- bind_rows(gnv, jax)

if (nrow(mom_baby_link_all_sites) != n_distinct(mom_baby_link_all_sites$part_id_infant)) {
  stop("Stage 01 QC failed: combined linkage is not one row per infant.")
}

if (any(is.na(mom_baby_link_all_sites$delivery_id))) {
  stop("Stage 01 QC failed: missing delivery_id.")
}

write_combined_rda(
  mom_baby_link_all_sites,
  "mom_baby_link_all_sites",
  "mom_baby_link_all_sites",
  working_dir
)

message(
  "ID linkage complete: ",
  n_distinct(mom_baby_link_all_sites$part_id_infant), " infants; ",
  n_distinct(mom_baby_link_all_sites$delivery_id), " delivery episodes."
)
