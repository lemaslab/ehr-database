suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(purrr)
  library(lubridate)
  library(yaml)
})

`%||%` <- function(x, y) if (is.null(x)) y else x

find_repo_root <- function(start = getwd()) {
  cur <- normalizePath(start, winslash = "/", mustWork = FALSE)
  for (i in seq_len(10)) {
    if (file.exists(file.path(cur, "config", "dataset_registry.yml"))) return(cur)
    parent <- dirname(cur)
    if (parent == cur) break
    cur <- parent
  }
  normalizePath(start, winslash = "/", mustWork = FALSE)
}

load_dataset_registry <- function(registry_path = NULL) {
  if (is.null(registry_path)) {
    registry_path <- file.path(find_repo_root(), "config", "dataset_registry.yml")
  }
  yaml::read_yaml(registry_path)
}

list_recursive_files <- function(root) {
  list.files(root, recursive = TRUE, full.names = TRUE, include.dirs = FALSE)
}

extract_path_metadata <- function(path) {
  tibble(
    path = path,
    year = stringr::str_match(path, "/(20\\d{2})/")[, 2],
    dataset_version = stringr::str_match(path, "/(dataset_\\d{2}_\\d{4})/")[, 2]
  )
}

score_file_candidates <- function(paths, preferred = list()) {
  if (length(paths) == 0) return(tibble())
  meta <- extract_path_metadata(paths)
  meta %>%
    mutate(
      score = 0L,
      score = score + if_else(!is.null(preferred$year) & year == preferred$year, 100L, 0L),
      score = score + if_else(!is.null(preferred$dataset_version) & dataset_version == preferred$dataset_version, 100L, 0L)
    ) %>%
    arrange(desc(score), desc(year), desc(dataset_version), path)
}

find_dataset_file <- function(dataset_key, component_key, site = "GNV", registry = NULL) {
  registry <- registry %||% load_dataset_registry()
  root <- registry$legacy_raw$base_dir
  component_def <- registry$datasets[[dataset_key]]$components[[component_key]]
  site_def <- component_def[[site]]

  if (is.null(site_def)) {
    stop("No site definition for ", dataset_key, " / ", component_key, " / ", site)
  }

  patterns <- site_def$filename_patterns
  files <- list_recursive_files(root)
  matches <- purrr::keep(files, ~ any(stringr::str_detect(basename(.x), patterns)))

  if (length(matches) == 0) {
    stop("No matching files found for ", dataset_key, " / ", component_key, " / ", site)
  }

  ranked <- score_file_candidates(matches, site_def$preferred %||% list())
  ranked$path[[1]]
}

rename_using_schema_map <- function(df, schema_map, strict = FALSE) {
  if (is.null(schema_map)) return(df)
  current_names <- names(df)

  for (new_name in names(schema_map)) {
    legacy_names <- schema_map[[new_name]]
    hit <- legacy_names[legacy_names %in% current_names][1]
    if (!is.na(hit)) {
      df <- df %>% rename(!!new_name := !!sym(hit))
      current_names[current_names == hit] <- new_name
    } else if (isTRUE(strict)) {
      stop("Missing required column for standardized field: ", new_name)
    }
  }
  df
}

read_standardized_dataset <- function(dataset_key,
                                      component_key,
                                      site = "GNV",
                                      registry = NULL,
                                      col_types = cols(),
                                      strict_schema = FALSE,
                                      verbose = TRUE) {
  registry <- registry %||% load_dataset_registry()
  path <- find_dataset_file(dataset_key, component_key, site, registry)
  df <- readr::read_csv(path, col_types = col_types, show_col_types = FALSE)

  schema_map <- registry$datasets[[dataset_key]]$components[[component_key]]$schema_map
  df <- rename_using_schema_map(df, schema_map, strict = strict_schema)

  if (isTRUE(verbose)) {
    message("[", dataset_key, "::", component_key, "::", site, "] ", path)
  }

  list(path = path, data = df)
}

parse_flexible_date <- function(x) {
  suppressWarnings(as.Date(parse_date_time(x, orders = c("mdy", "ymd", "mdy HMS", "mdy HM", "ymd HMS", "ymd HM"))))
}

parse_flexible_datetime <- function(x) {
  suppressWarnings(parse_date_time(x, orders = c("mdy HMS", "mdy HM", "ymd HMS", "ymd HM", "mdy", "ymd")))
}

get_processed_dir <- function(site = NULL, subdir = NULL, working_dir = NULL) {
  root <- working_dir %||% find_repo_root()
  path <- file.path(root, "data", "processed")
  if (!is.null(site)) path <- file.path(path, site)
  if (!is.null(subdir)) path <- file.path(path, subdir)
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  path
}

export_processed_dataset <- function(df, dataset_name, site = NULL, working_dir = NULL) {
  out_dir <- get_processed_dir(site = site, working_dir = working_dir)
  stamp <- format(Sys.Date(), "%Y%m%d")
  csv_path <- file.path(out_dir, paste0(dataset_name, "_", stamp, ".csv"))
  rda_path <- file.path(out_dir, paste0(dataset_name, "_", stamp, ".rda"))
  readr::write_csv(df, csv_path, na = "")
  obj_name <- dataset_name
  e <- new.env(parent = emptyenv())
  assign(obj_name, df, envir = e)
  save(list = obj_name, file = rda_path, envir = e)
  invisible(list(csv = csv_path, rda = rda_path))
}
