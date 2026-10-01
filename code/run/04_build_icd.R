# Stage 04: maternal and infant ICD datasets
#
# Runs the existing production processors after canonical linkage is validated.

required <- c(
  "code/process_data/run_mom_icd.R",
  "code/process_data/run_infant_icd.R"
)
missing <- required[!file.exists(required)]
if (length(missing)) stop("Missing Stage 04 runner(s): ", paste(missing, collapse = ", "))

message("Stage 04a: maternal ICD")
source("code/process_data/run_mom_icd.R")

message("Stage 04b: infant ICD")
source("code/process_data/run_infant_icd.R")

message("Stage 04 ICD processing complete.")
