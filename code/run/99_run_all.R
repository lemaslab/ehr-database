# Run canonical ID/linkage pipeline
#
# Default: reuse successful cached site/dataset stages.
# Force rebuild:
#   Sys.setenv(EHR_FORCE = "true")
#   source("code/run/99_run_all.R")
# Then reset:
#   Sys.unsetenv("EHR_FORCE")

message("=== EHR ID/LINKAGE PIPELINE ===")
message("Stage 01: mother-infant-delivery linkage")
source("code/run/01_build_id_linkage.R")

message("Stage 02: delivery encounters")
source("code/run/02_build_delivery_encounter.R")

message("=== PIPELINE COMPLETE ===")
