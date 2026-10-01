suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(purrr)
  library(yaml)
})

# -----------------------------
# Utilities
# -----------------------------

find_repo_root <- function(start = getwd()) {
  cur <- normalizePath(start, winslash = "/", mustWork = FALSE)
  for (i in 1:10) {
    if (file.exists(file.path(cur, "config", "dataset_registry.yml"))) return(cur)
    parent <- dirname(cur)
    if (parent == cur) break
    cur <- parent
  }
  return(getwd())
}

load_registry <- function(registry_path = NULL) {
  if (is.null(registry_path)) {
    root <- find_repo_root()
    registry_path <- file.path(root, "config", "dataset_registry.yml")
  }
  yaml::read_yaml(registry_path)
}

# -----------------------------
# Recursive file search
# -----------------------------

list_recursive_files <- function(root) {
  list.files(root, recursive = TRUE, full.names = TRUE, include.dirs = FALSE)
}

extract_meta_from_path <- function(path) {
  year <- str_match(path, "/(20\\d{2})/")[,2]
  ds   <- str_match(path, "/(dataset_\\d{2}_\\d{4})/")[,2]
  tibble(path = path, year = year, dataset = ds)
}

score_candidates <- function(paths, preferred = list()) {
  if (length(paths) == 0) return(tibble())
  meta <- extract_meta_from_path(paths)
  score <- rep(0, nrow(meta))
  if (!is.null(preferred$year)) {
    score <- score + ifelse(meta$year == preferred$year, 100, 0)
  }
  if (!is.null(preferred$dataset_version)) {
    score <- score + ifelse(meta$dataset == preferred$dataset_version, 100, 0)
  }
  meta$score <- score
  meta %>% arrange(desc(score))
}

find_dataset_file <- function(dataset_key, component, site = "GNV", registry = NULL) {
  registry <- registry %||% load_registry()
  root <- registry$legacy_raw$base_dir
  comp <- registry$datasets[[dataset_key]]$components[[component]]
  site_def <- comp[[site]]
  patterns <- site_def$filename_patterns

  files <- list_recursive_files(root)
  matches <- keep(files, ~ any(str_detect(basename(.x), patterns)))

  if (length(matches) == 0) stop("No matches for ", dataset_key, "::", component)

  ranked <- score_candidates(matches, site_def$preferred)
  ranked$path[[1]]
}

# -----------------------------
# Schema mapping
# -----------------------------

rename_using_map <- function(df, schema_map, strict = FALSE) {
  nm <- names(df)
  for (new_name in names(schema_map)) {
    olds <- schema_map[[new_name]]
    hit <- olds[olds %in% nm][1]
    if (!is.na(hit)) {
      df <- df %>% rename(!!new_name := !!sym(hit))
    } else if (isTRUE(strict)) {
      stop("Missing required column for ", new_name)
    }
  }
  df
}

# -----------------------------
# Read + standardize
# -----------------------------

read_standardized_dataset <- function(dataset_key, component, site = "GNV", registry = NULL, col_types = cols()) {
  registry <- registry %||% load_registry()
  path <- find_dataset_file(dataset_key, component, site, registry)
  df <- read_csv(path, col_types = col_types)

  schema_map <- registry$datasets[[dataset_key]]$components[[component]]$schema_map
  if (!is.null(schema_map)) {
    df <- rename_using_map(df, schema_map)
  }

  list(path = path, data = df)
}

# -----------------------------
# Flexible date parsing
# -----------------------------

parse_flexible_date <- function(x) {
  suppressWarnings(as.Date(lubridate::parse_date_time(x, orders = c("mdy", "ymd", "mdy HMS", "ymd HMS"))))
}

parse_flexible_datetime <- function(x) {
  suppressWarnings(lubridate::parse_date_time(x, orders = c("mdy HMS", "mdy HM", "ymd HMS", "ymd HM")))
}

# -----------------------------
# Output helpers
# -----------------------------

get_processed_dir <- function(site = NULL) {
  root <- find_repo_root()
  base <- file.path(root, "data", "processed")
  if (!is.null(site)) base <- file.path(base, site)
  if (!dir.exists(base)) dir.create(base, recursive = TRUE, showWarnings = FALSE)
  base
}
