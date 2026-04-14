process_delivery_encounter_simple_v1 <- function(site,
                                                working_dir = getwd(),
                                                mom_baby_link_df = NULL) {
  
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
  })
  
  site <- toupper(site)
  
  if (!site %in% c("GNV", "JAX")) {
    stop("Unsupported site")
  }
  
  message("=== PROCESSING DELIVERY ENCOUNTER: ", site, " ===")
  
  # ===============================
  # GNV
  # ===============================
  
  if (site == "GNV") {
    
    base_path <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021"
    fpath <- file.path(base_path, "baby_mom_at_birth_with_payer.csv")
    
    if (!file.exists(fpath)) {
      stop("[GNV] file not found: ", fpath)
    }
    
    message("[GNV] reading delivery_encounter")
    
    delivery <- read_csv(fpath, show_col_types = FALSE) %>%
      rename(
        part_id_mom_tmp = dplyr::any_of(c("Deidentified_mom_ID", "deidentified_mom_id")),
        delivery_date = dplyr::any_of(c("Delivery Date", "delivery_date")),
        gestational_age = dplyr::any_of(c("Gestational Age", "gestational_age")),
        delivery_type = dplyr::any_of(c("Delivery Method", "delivery_method"))
      )
  }
  
  # ===============================
  # JAX
  # ===============================
  
  if (site == "JAX") {
    
    fpath <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/baby_mom_at_birth_Jax.csv"
    
    if (!file.exists(fpath)) {
      stop("[JAX] file not found: ", fpath)
    }
    
    message("[JAX] reading baby_mom_at_birth_Jax")
    
    delivery <- read_csv(fpath, show_col_types = FALSE) %>%
      rename(
        part_id_mom_tmp = dplyr::any_of(c("Deidentified_mom_ID", "deidentified_mom_id")),
        part_id_infant_tmp = dplyr::any_of(c("Deidentified_baby_ID", "deidentified_baby_id")),
        delivery_date = dplyr::any_of(c("Delivery Date", "delivery_date")),
        gestational_age = dplyr::any_of(c("Gestational Age", "gestational_age")),
        delivery_type = dplyr::any_of(c("Delivery Method", "delivery_method"))
      )
  }
  
  # ===============================
  # ID STANDARDIZATION
  # ===============================
  
  site_map <- c("GNV" = "AC", "JAX" = "DC")
  site_prefix <- site_map[[site]]
  
  delivery <- delivery %>%
    mutate(
      part_id_mom = paste0(site_prefix, "-mom-", part_id_mom_tmp),
      site = site
    )
  
  # ===============================
  # JOIN TO MOM-BABY LINK
  # ===============================
  
  if (!is.null(mom_baby_link_df)) {
    
    message("[", site, "] joining to mom_baby_link")
    
    delivery <- mom_baby_link_df %>%
      select(part_id_mom, part_id_infant, delivery_id, site) %>%
      left_join(delivery, by = "part_id_mom")
    
  } else {
    warning("[", site, "] mom_baby_link_df not provided")
  }
  
  # ===============================
  # DATE PARSING
  # ===============================
  
  delivery <- delivery %>%
    mutate(
      delivery_date = suppressWarnings(parse_date_time(
        delivery_date,
        orders = c("mdy HMS", "ymd HMS", "mdy", "ymd")
      ))
    )
  
  # ===============================
  # FINAL DATASET
  # ===============================
  
  delivery_encounter <- delivery %>%
    select(
      delivery_id,
      part_id_mom,
      part_id_infant,
      delivery_date,
      gestational_age,
      delivery_type,
      site
    ) %>%
    distinct()
  
  # ===============================
  # OUTPUT (SITE LEVEL)
  # ===============================
  
  date_tag <- format(Sys.Date(), "%Y%m%d")
  
  out_dir <- file.path(working_dir, "data", "processed", site)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  file_base <- paste0("delivery_encounter_", site, "_", date_tag)
  
  save(delivery_encounter, file = file.path(out_dir, paste0(file_base, ".rda")))
  write_csv(delivery_encounter, file.path(out_dir, paste0(file_base, ".csv")), na = "")
  
  message("[", site, "] rows: ", nrow(delivery_encounter))
  
  return(delivery_encounter)
}
