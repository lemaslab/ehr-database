# Delivery Encounter Runner v2

source("code/functions/utils_load_latest_dataset.R")
source("code/functions/utils_write_dataset.R")
source("code/functions/process_delivery_encounter_simple_v1.R")

library(dplyr)

working_dir <- getwd()

# Load mom_baby_link
mom_baby_link <- load_latest_dataset("mom_baby_link_all_sites", working_dir)

gnv_mb <- mom_baby_link %>% filter(site == "GNV")
jax_mb <- mom_baby_link %>% filter(site == "JAX")

# Run processing
gnv <- process_delivery_encounter_simple_v1("GNV", working_dir, gnv_mb)
jax <- process_delivery_encounter_simple_v1("JAX", working_dir, jax_mb)

delivery_encounter <- bind_rows(gnv, jax)

# Variable comparison
cat("\n==== VARIABLE COMPARISON ====\n")
cat("Common:\n", paste(intersect(names(gnv), names(jax)), collapse=", "), "\n")
cat("GNV-only:\n", paste(setdiff(names(gnv), names(jax)), collapse=", "), "\n")
cat("JAX-only:\n", paste(setdiff(names(jax), names(gnv)), collapse=", "), "\n")

# Write output
write_dataset(delivery_encounter, "delivery_encounter_all_sites", working_dir, "COMBINED")
