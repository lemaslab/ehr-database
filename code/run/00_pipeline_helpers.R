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

# Run-scoped diagnostics. Logs contain schemas and aggregate counts only.
ehr_log_init <- function(run_id, root = getwd()) {
  path <- file.path(root, "logs", paste0("run_", run_id))
  if (dir.exists(path)) stop("Log directory already exists: ", path)
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  Sys.setenv(EHR_LOG_DIR = normalizePath(path, winslash = "/", mustWork = TRUE))
  path
}
ehr_log_dir <- function() Sys.getenv("EHR_LOG_DIR", unset = "")
ehr_log_append <- function(file, values) {
  dir <- ehr_log_dir()
  if (!nzchar(dir)) return(invisible(NULL))
  path <- file.path(dir, file)
  line <- as.data.frame(values, stringsAsFactors = FALSE)
  utils::write.table(line, path, sep = ",", row.names = FALSE,
                     col.names = !file.exists(path), append = file.exists(path),
                     qmethod = "double", na = "")
  invisible(path)
}
ehr_audit_schema <- function(df, dataset, checkpoint = "output", site = NA_character_) {
  if (!is.data.frame(df)) return(invisible(NULL))
  dir <- ehr_log_dir()
  if (!nzchar(dir)) return(invisible(NULL))
  schema <- data.frame(
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    dataset = dataset, checkpoint = checkpoint, site = site,
    rows = nrow(df), columns = ncol(df),
    variable = names(df),
    type = vapply(df, function(x) paste(class(x), collapse = "/"), character(1)),
    missing_n = vapply(df, function(x) sum(is.na(x)), numeric(1)),
    stringsAsFactors = FALSE)
  path <- file.path(dir, "schema_inventory.csv")
  utils::write.table(schema, path, sep = ",", row.names = FALSE,
                     col.names = !file.exists(path), append = file.exists(path),
                     qmethod = "double", na = "")
  invisible(schema)
}
