# Stage 02: delivery encounters using validated infant-level linkage

source("code/run/00_pipeline_helpers.R")
source("code/functions/process_mom_baby_link.R")
source("code/functions/process_delivery_encounter.R")

suppressPackageStartupMessages(library(dplyr))

working_dir <- getwd()

get_link <- function(site) {
  load_or_build(
    site, "mom_baby_link",
    function() process_mom_baby_link(site, working_dir),
    working_dir
  )
}

gnv_link <- get_link("GNV")
jax_link <- get_link("JAX")

gnv <- load_or_build(
  "GNV", "delivery_encounter",
  function() process_delivery_encounter("GNV", working_dir, gnv_link),
  working_dir
)

jax <- load_or_build(
  "JAX", "delivery_encounter",
  function() process_delivery_encounter("JAX", working_dir, jax_link),
  working_dir
)

delivery_encounter_all_sites <- bind_rows(gnv, jax)

if (nrow(delivery_encounter_all_sites) !=
    n_distinct(delivery_encounter_all_sites$part_id_infant)) {
  stop("Stage 02 QC failed: delivery encounter is not one row per infant.")
}

if (any(is.na(delivery_encounter_all_sites$delivery_id))) {
  stop("Stage 02 QC failed: missing delivery_id.")
}

write_combined_rda(
  delivery_encounter_all_sites,
  "delivery_encounter_all_sites",
  "delivery_encounter_all_sites",
  working_dir
)

message(
  "Delivery encounters complete: ",
  n_distinct(delivery_encounter_all_sites$part_id_infant), " infants; ",
  n_distinct(delivery_encounter_all_sites$delivery_id), " delivery episodes."
)
