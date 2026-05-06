process_mom_baby_link <- function(site, working_dir = getwd()) {
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

  site_id_prefix <- site_map[[site]]  # used ONLY for IDs

  if (site == "GNV") {
    linkage_path <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/baby_mom_link.csv"
    birth_path   <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/baby_mom_at_birth_with_payer.csv"
  } else if (site == "JAX") {
    linkage_path <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/baby_mom_link_Jax.csv"
    birth_path   <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/baby_mom_at_birth_Jax.csv"
  }

  if (!file.exists(linkage_path)) stop("Missing linkage file: ", linkage_path)
  if (!file.exists(birth_path)) stop("Missing birth file: ", birth_path)

  message("[", site, "] linkage: ", linkage_path)
  message("[", site, "] birth: ", birth_path)

  linkage <- read_csv(linkage_path, show_col_types = FALSE) %>%
    rename(
      part_id_mom_tmp = dplyr::any_of(c("Deidentified_mom_ID", "deidentified_mom_id")),
      part_id_infant_tmp = dplyr::any_of(c("Deidentified_baby_ID", "deidentified_baby_id"))
    ) %>%
    mutate(
      part_id_mom = paste0(site_id_prefix, "-mom-", part_id_mom_tmp),
      part_id_infant = paste0(site_id_prefix, "-infant-", part_id_infant_tmp)
    ) %>%
    select(part_id_mom, part_id_infant)

  birth <- read_csv(birth_path, show_col_types = FALSE) %>%
    rename(
      part_id_infant_tmp = dplyr::any_of(c("Deidentified_baby_ID", "deidentified_baby_id")),
      part_dob_infant_raw = dplyr::any_of(c("Deid-Delivery Datetime", "date_of_delivery", "Date of delivery"))
    ) %>%
    mutate(
      part_id_infant = paste0(site_id_prefix, "-infant-", part_id_infant_tmp),
      part_dob_infant = suppressWarnings(parse_date_time(
        part_dob_infant_raw,
        orders = c("mdy HMS", "mdy HM", "ymd HMS", "ymd HM", "mdy", "ymd")
      ))
    ) %>%
    select(part_id_infant, part_dob_infant)

  mom_baby_link <- linkage %>%
    left_join(birth, by = "part_id_infant") %>%
    arrange(part_id_mom, part_dob_infant) %>%
    group_by(part_id_mom) %>%
    mutate(temp_id = cumsum(c(1, diff(part_dob_infant) > 3))) %>%
    ungroup() %>%
    mutate(
      delivery_id_num = dense_rank(paste(part_id_mom, temp_id, sep = "_")),
      delivery_id = paste0(site_id_prefix, "-delivery-", delivery_id_num),
      site = site
    ) %>%
    select(part_id_mom, part_id_infant, delivery_id, part_dob_infant, site)

  date_tag <- format(Sys.Date(), "%Y%m%d")

  out_dir <- file.path(working_dir, "data", "processed", site)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  file_base <- paste0("mom_baby_link_", site, "_", date_tag)

  save(mom_baby_link, file = file.path(out_dir, paste0(file_base, ".rda")))
  readr::write_csv(mom_baby_link, file.path(out_dir, paste0(file_base, ".csv")), na = "")

  message("[", site, "] rows: ", nrow(mom_baby_link))
  message("[", site, "] missing DOB: ", sum(is.na(mom_baby_link$part_dob_infant)))
  mom_baby_link
}
