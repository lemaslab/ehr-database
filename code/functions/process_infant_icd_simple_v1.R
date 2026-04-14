process_infant_icd_simple_v1 <- function(site, working_dir = getwd()) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
  })

  site <- toupper(site)

  site_map <- c("GNV" = "AC", "JAX" = "DC")
  if (!site %in% names(site_map)) stop("Unsupported site")

  site_id_prefix <- site_map[[site]]

  if (site == "GNV") {
    base_icd <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/baby_data"
    neonatal_file <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2022/dataset_07_2022/neonatal_defects_release.csv"
  } else {
    base_icd <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/baby_data"
    neonatal_file <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/neonatal_defects_release_Jax.csv"
  }

  icd_files <- list.files(base_icd, full.names = TRUE)

  icd_list <- lapply(icd_files, function(f) {
    message("[", site, "] reading: ", basename(f))

    read_csv(f, show_col_types = FALSE) %>%
      rename(
        part_id_infant_tmp = dplyr::any_of(c("Deidentified_baby_ID", "deidentified_baby_id")),
        dx_code = dplyr::any_of(c("Diagnosis Code", "diagnosis_code")),
        dx_date_raw = dplyr::any_of(c("Diagnosis Start Date", "diagnosis_start_date")),
        dx_type = dplyr::any_of(c("Diagnosis Type", "diagnosis_type"))
      ) %>%
      mutate(source = "baby_data")
  })

  if (file.exists(neonatal_file)) {
    message("[", site, "] reading neonatal_defects")

    neonatal_df <- read_csv(neonatal_file, show_col_types = FALSE) %>%
      rename(
        part_id_infant_tmp = dplyr::any_of(c("Deidentified_baby_ID", "deidentified_baby_id")),
        dx_code = dplyr::any_of(c("Diagnosis Code", "diagnosis_code")),
        dx_date_raw = dplyr::any_of(c("Diagnosis Start Date", "diagnosis_start_date"))
      ) %>%
      mutate(dx_type = "neonatal_defect", source = "neonatal_defects")

    icd_list <- c(icd_list, list(neonatal_df))
  }

  infant_icd <- bind_rows(icd_list) %>%
    mutate(
      part_id_infant = paste0(site_id_prefix, "-infant-", part_id_infant_tmp),
      dx_date = suppressWarnings(parse_date_time(dx_date_raw,
        orders = c("mdy HMS", "mdy HM", "ymd HMS", "ymd HM", "mdy", "ymd")
      )),
      site = site
    ) %>%
    select(part_id_infant, dx_code, dx_date, dx_type, site, source)

  date_tag <- format(Sys.Date(), "%Y%m%d")
  out_dir <- file.path(working_dir, "data", "processed", site)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  file_base <- paste0("infant_icd_", site, "_", date_tag)

  save(infant_icd, file = file.path(out_dir, paste0(file_base, ".rda")))
  write_csv(infant_icd, file.path(out_dir, paste0(file_base, ".csv")), na = "")

  infant_icd
}
