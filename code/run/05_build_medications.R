# Stage 05: maternal medication datasets
#
# Runs the existing inpatient and outpatient maternal medication processors.

required <- c(
  "code/process_data/run_mom_medications_ip.R",
  "code/process_data/run_mom_medications_op.R"
)
missing <- required[!file.exists(required)]
if (length(missing)) {
  stop(
    "Missing Stage 05 runner(s): ", paste(missing, collapse = ", "),
    ". These files must be committed to the repository for a portable full pipeline."
  )
}

message("Stage 05a: maternal inpatient medications")
source("code/process_data/run_mom_medications_ip.R")

message("Stage 05b: maternal outpatient medications")
source("code/process_data/run_mom_medications_op.R")

message("Stage 05 medication processing complete.")
