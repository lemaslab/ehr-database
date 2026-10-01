# Stage 06: clinical note metadata
#
# Builds the combined GNV/JAX prenatal, delivery, and postnatal note metadata.
# Note text standardization is intentionally not sourced here because
# run_standardize_gnv_delivery_notes.R is currently configured as a
# 100-note troubleshooting workflow rather than a production processor.

runner <- "code/process_data/run_clinical_notes_metadata.R"
if (!file.exists(runner)) stop("Missing Stage 06 runner: ", runner)

message("Stage 06: clinical notes metadata")
source(runner)

message("Stage 06 clinical note metadata complete.")
