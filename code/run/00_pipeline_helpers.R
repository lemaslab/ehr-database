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
  # All canonical combined tables use the same output policy.
  source("code/functions/utils_write_dataset.R", local=TRUE)
  result <- write_dataset(object, dataset_name=object_name,
    working_dir=working_dir, subdir="COMBINED")
  message("[output] ", result$rda)
  invisible(result$rda)
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

# Output schema and canonical identifier QC: one row per checked variable.
# Missing IDs on role-inapplicable notes must not be treated as a failure.
ehr_audit_ids <- function(df, dataset, site=NA_character_) {
  keys <- intersect(c("part_id_mom","part_id_infant","delivery_id","note_uid_global"),names(df))
  for (key in keys) {
    x <- as.character(df[[key]])
    applicable <- rep(TRUE,length(x))
    if ("participant_role" %in% names(df)) {
      if (key=="part_id_mom") applicable <- as.character(df$participant_role)=="mom"
      if (key=="part_id_infant") applicable <- as.character(df$participant_role) %in% c("baby","infant")
    }
    missing <- sum(applicable & (is.na(x) | !nzchar(trimws(x))))
    bad <- NA_integer_
    if (key %in% c("part_id_mom","part_id_infant") && "site" %in% names(df)) {
      prefix <- ifelse(as.character(df$site)=="GNV","AC-",
                       ifelse(as.character(df$site)=="JAX","DC-",NA_character_))
      prefix <- paste0(prefix,if(key=="part_id_mom")"mom-" else "infant-")
      bad <- sum(applicable & !is.na(x) & !is.na(prefix) &
                 !startsWith(x,prefix))
    }
    status <- if(missing>0 || (!is.na(bad)&&bad>0)) "WARNING" else "PASS"
    message("[ID QC] ",dataset," | ",key," | ",status,
            " | missing=",missing," | invalid_prefix=",bad)
    ehr_log_append("identifier_qc.csv",list(
      dataset=dataset,site=site,variable=key,applicable_rows=sum(applicable),
      missing_rows=missing,invalid_prefix_rows=bad,status=status))
  }
}
# Human-readable raw-to-canonical terminology (a reference mapping, not a
# claim that every possible raw column is present in each source).
ehr_variable_dictionary <- function() {
  data.frame(raw_label=c("Deidentified_mom_ID","deidentified_mom_id",
    "Deidentified_baby_ID","deidentified_baby_id",
    "Deidentified_note_key","Taken Datetime","date_of_delivery"),
    canonical_label=c("part_id_mom","part_id_mom","part_id_infant",
    "part_id_infant","note_id_source","date_med_taken","delivery_date"),
    notes=c("GNV/DC site prefixes applied","GNV/DC site prefixes applied",
      "GNV/DC site prefixes applied","GNV/DC site prefixes applied",
      "Source-dependent parsing; verify processor",
      "Medication timestamp","Canonical delivery encounter date"),
    stringsAsFactors=FALSE)
}
