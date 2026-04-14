process_mom_baby_link_simple_v3 <- function(site, working_dir = getwd()) {
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
  })

  site <- toupper(site)

  site_map <- c("GNV" = "AC", "JAX" = "DC")

  if (!site %in% names(site_map)) {
    stop("Unsupported site: ", site)
  }

  site_clean <- site_map[[site]]

  if (site == "GNV") {
    linkage_path <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/baby_mom_link.csv"
    birth_path   <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/baby_mom_at_birth_with_payer.csv"
  } else if (site == "JAX") {
    linkage_path <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/baby_mom_link_Jax.csv"
    birth_path   <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/baby_mom_at_birth_Jax.csv"
  }

  if (!file.exists(linkage_path)) stop("Missing linkage file: ", linkage_path)
  if (!file.exists(birth_path)) stop("Missing birth file: ", birth_path)

  message("[", site_clean, "] linkage: ", linkage_path)
  message("[", site_clean, "] birth: ", birth_path)

  linkage <- read_csv(linkage_path, show_col_types = FALSE) %>%
    rename(
      part_id_mom_tmp = dplyr::any_of(c("Deidentified_mom_ID", "deidentified_mom_id")),
      part_id_infant_tmp = dplyr::any_of(c("Deidentified_baby_ID", "deidentified_baby_id"))
    ) %>%
    mutate(
      part_id_mom = paste0(site_clean, "-mom-", part_id_mom_tmp),
      part_id_infant = paste0(site_clean, "-infant-", part_id_infant_tmp)
    ) %>%
    select(part_id_mom, part_id_infant)

  birth <- read_csv(birth_path, show_col_types = FALSE) %>%
    rename(
      part_id_infant_tmp = dplyr::any_of(c("Deidentified_baby_ID", "deidentified_baby_id")),
      part_dob_infant_raw = dplyr::any_of(c("Deid-Delivery Datetime", "date_of_delivery", "Date of delivery"))
    ) %>%
    mutate(
      part_id_infant = paste0(site_clean, "-infant-", part_id_infant_tmp),
      part_dob_infant = suppressWarnings(parse_date_time(
        part_dob_infant_raw,
        orders = c("mdy HMS", "mdy HM", "ymd HMS", "ymd HM", "mdy", "ymd")
      ))
    ) %>%
    select(part_id_infant, part_dob_infant)

  df <- linkage %>%
    left_join(birth, by = "part_id_infant") %>%
    arrange(part_id_mom, part_dob_infant) %>%
    group_by(part_id_mom) %>%
    mutate(
      delivery_num = cumsum(if_else(
        is.na(lag(part_dob_infant)) |
          as.numeric(difftime(part_dob_infant, lag(part_dob_infant), units = "days")) > 3,
        1,
        0
      )),
      delivery_id = paste0(site_clean, "-delivery-", delivery_num),
      site = site_clean
    ) %>%
    ungroup() %>%
    select(part_id_mom, part_id_infant, delivery_id, part_dob_infant, site)

  date_tag <- format(Sys.Date(), "%Y%m%d")

  out_dir <- file.path(working_dir, "data", "processed", site_clean)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  file_base <- paste0("mom_baby_link_", site_clean, "_", date_tag)

  save(df, file = file.path(out_dir, paste0(file_base, ".rda")))
  readr::write_csv(df, file.path(out_dir, paste0(file_base, ".csv")), na = "")

  message("[", site_clean, "] rows: ", nrow(df))
  message("[", site_clean, "] missing DOB: ", sum(is.na(df$part_dob_infant)))
  df
}
