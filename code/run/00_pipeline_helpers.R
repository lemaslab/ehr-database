# Helpers for resumable ID/linkage pipeline

pipeline_cache_dir <- function(working_dir = getwd()) {
  path <- file.path(working_dir, "data", "cache", "id_linkage_pipeline")
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  path
}

pipeline_force <- function() {
  tolower(Sys.getenv("EHR_FORCE", unset = "false")) %in% c("1", "true", "yes", "y")
}

cache_path <- function(site, dataset, working_dir = getwd()) {
  file.path(
    pipeline_cache_dir(working_dir),
    paste0(tolower(site), "_", dataset, ".rds")
  )
}

atomic_save_rds <- function(object, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp <- paste0(path, ".tmp")
  saveRDS(object, tmp)
  if (file.exists(path)) file.remove(path)
  if (!file.rename(tmp, path)) {
    file.copy(tmp, path, overwrite = TRUE)
    file.remove(tmp)
  }
  invisible(path)
}

load_or_build <- function(site, dataset, build_fn, working_dir = getwd()) {
  path <- cache_path(site, dataset, working_dir)

  if (file.exists(path) && !pipeline_force()) {
    message("[cache] loading ", path)
    return(readRDS(path))
  }

  message("[build] ", site, " / ", dataset)
  object <- build_fn()
  atomic_save_rds(object, path)
  message("[cache] saved ", path)
  object
}

write_combined_rda <- function(object, object_name, stem, working_dir = getwd()) {
  out_dir <- file.path(working_dir, "data", "processed", "COMBINED")
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  date_tag <- format(Sys.Date(), "%Y%m%d")
  path <- file.path(out_dir, paste0(stem, "_", date_tag, ".rda"))

  env <- new.env(parent = emptyenv())
  assign(object_name, object, envir = env)
  save(list = object_name, file = path, envir = env)

  message("[output] ", path)
  invisible(path)
}
