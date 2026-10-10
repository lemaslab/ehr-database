# Stage 07: maternal and infant laboratory harmonization across GNV + JAX.
# source("code/run/07_build_labs.R")
# Pilot: Sys.setenv(EHR_LAB_N_MAX="10000")
# Full: Sys.unsetenv("EHR_LAB_N_MAX")
# Linkage intentionally deferred until warehouse repairs IDs.
source("code/functions/process_structured_labs.R")
# Candidate ten-year clinical lab sources; not yet canonical-ID validated.
root <- Sys.getenv("EHR_LAB_INPUT_ROOT",
  "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data")
paths <- c(
  mom_gnv = file.path(root, "2021/dataset_10_2021/mom_labs.csv"),
  mom_jax = file.path(root, "2025/dataset_04_2025/mom_labs_Jax.csv"),
  baby_gnv = file.path(root, "2023/dataset_01_2023/infant_labs_deid_update.csv"),
  baby_jax = file.path(root, "2025/dataset_04_2025/infant_labs_deid_update_Jax.csv")
)
cat("\n==== LAB SOURCE FILES ====\n")
print(data.frame(dataset=names(paths),source=unname(paths),
  exists=unname(file.exists(paths)),row.names=NULL))
if (any(!file.exists(paths)))
  stop("Missing laboratory source CSV(s): ", paste(paths[!file.exists(paths)], collapse = ", "))
if (tolower(Sys.getenv("EHR_LAB_LINKAGE_MODE", "defer")) != "defer")
  stop("Stage 07 only supports deferred linkage until canonical IDs are validated.")
limit <- Sys.getenv("EHR_LAB_N_MAX", "")
n_max <- if (nzchar(limit)) suppressWarnings(as.integer(limit)) else Inf
if (nzchar(limit) && (is.na(n_max) || n_max <= 0))
  stop("EHR_LAB_N_MAX must be a positive integer.")
if (!requireNamespace("dplyr", quietly = TRUE)) stop("Requires dplyr.")
out_root <- file.path(getwd(), "data", "processed")
tag <- format(Sys.time(), "%Y%m%d_%H%M%S")
save_local <- function(data, object_name, folder) {
  dir.create(folder, recursive = TRUE, showWarnings = FALSE)
  target <- file.path(folder, paste0(object_name, "_", tag, ".rda"))
  if (file.exists(target)) stop("Refusing to overwrite: ", target)
  e <- new.env(parent = emptyenv())
  assign(object_name, data, envir = e)
  tmp <- tempfile(pattern = ".labs_stage_", tmpdir = folder)
  on.exit(unlink(tmp), add = TRUE)
  save(list = object_name, file = tmp, envir = e, compress = TRUE)
  if (!isTRUE(file.info(tmp)$size > 0) || !file.rename(tmp, target))
    stop("Unable to save local RDA: ", target)
  message("Saved local RDA: ", target)
  target
}
site_rdas <- character()
for (key in names(paths)) {
  site <- if (endsWith(key, "gnv")) "GNV" else "JAX"
  maternal <- startsWith(key, "mom")
  role <- if (maternal) "mom" else "baby"
  object_name <- paste0(if (maternal) "mom" else "infant", "_labs_", tolower(site))
  data <- process_structured_labs(paths[[key]], site, role, n_max)
  site_rdas[key] <- save_local(data, object_name, file.path(out_root, site))
  rm(data)
  gc(verbose = FALSE)
}
if (tolower(Sys.getenv("EHR_LAB_COMBINE", "true")) %in% c("true", "yes", "1")) {
  for (group in c("mom", "baby")) {
    keys <- paste0(group, "_", c("gnv", "jax"))
    objects <- lapply(keys, function(key) {
      e <- new.env(parent = emptyenv())
      load(site_rdas[[key]], envir = e)
      object_name <- paste0(if (group == "mom") "mom" else "infant",
                            "_labs_", sub("^.*_", "", key))
      get(object_name, envir = e, inherits = FALSE)
    })
    combined <- dplyr::bind_rows(objects)
    stopifnot(nrow(combined) == sum(vapply(objects, nrow, integer(1))))
    name <- if (group == "mom") "mom_labs_all_sites" else "infant_labs_all_sites"
    save_local(combined, name, file.path(out_root, "COMBINED"))
    rm(objects, combined)
    gc(verbose = FALSE)
  }
}
message("Stage 07 complete: laboratory IDs remain unlinked pending warehouse correction.")
