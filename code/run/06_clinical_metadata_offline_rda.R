# Strictly offline clinical notes metadata: local CSV inputs, local RDA output.
# Before sourcing, set EHR_METADATA_INPUT_ROOT to a local folder containing
# the directory tree formerly under V:/FACULTY/DJLEMAS/EHR_Data_raw.
# The canonical mom_baby_link_all_sites_YYYYMMDD.rda must already be in
# data/processed/COMBINED. No CSV output, remote sync, or linkage rebuild.
local({
  root <- Sys.getenv("EHR_METADATA_INPUT_ROOT", unset = "")
  if (!nzchar(root) || !dir.exists(root))
    stop("Set EHR_METADATA_INPUT_ROOT to an existing LOCAL folder before source().")
  if (grepl("^//|^\\\\\\\\", root))
    stop("UNC/network paths are not permitted in strict offline mode.")
  if (toupper(substr(root, 1L, 2L)) == "V:")
    stop("V: is the mapped remote drive; use a locally stored copy.")
  vars <- c("EHR_OUTPUT_MODE", "EHR_NETWORK_SYNC", "EHR_REQUIRE_LOCAL_INPUT")
  old <- Sys.getenv(vars, unset = NA_character_)
  on.exit({
    for (i in seq_along(vars)) {
      if (is.na(old[i])) Sys.unsetenv(vars[i])
      else do.call(Sys.setenv, setNames(list(old[i]), vars[i]))
    }
  }, add = TRUE)
  Sys.setenv(EHR_OUTPUT_MODE = "pilot",
             EHR_NETWORK_SYNC = "false",
             EHR_REQUIRE_LOCAL_INPUT = "true")
  source("code/run/06_build_clinical_notes_metadata.R", local = FALSE)
})
