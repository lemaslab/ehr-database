process_infant_icd_simple_v2 <- function(site,
                                         working_dir = getwd(),
                                         mom_baby_link_df = NULL) {
  
  suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(lubridate)
  })
  
  site <- toupper(site)
  
  # ===============================
  # Site mapping (IDs only)
  # ===============================
  site_map <- c("GNV" = "AC", "JAX" = "DC")
  if (!site %in% names(site_map)) stop("Unsupported site")
  
  site_id_prefix <- site_map[[site]]
  
  # ===============================
  # LOAD ICD FILES (SITE-SPECIFIC)
  # ===============================
  
  if (site == "GNV") {
    
    data_directory <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/baby_data"
    
    file_map <- list(
      baby_asthma = "asthma",
      baby_ear_infection = "ear_infection",
      baby_eczema = "eczema",
      baby_food_allergy = "food_allergy",
      baby_hemangonia = "hemangonia",
      baby_nevus = "nevus",
      baby_obesity = "obesity",
      baby_sebor = "sebor",
      baby_toxicum = "toxicum"
    )
    
    icd_list <- lapply(names(file_map), function(fname) {
      
      fpath <- file.path(data_directory, paste0(fname, ".csv"))
      
      if (!file.exists(fpath)) {
        warning("[GNV] missing file: ", fpath)
        return(NULL)
      }
      
      message("[GNV] reading: ", fname)
      
      read_csv(fpath, show_col_types = FALSE) %>%
        mutate(dx_category = file_map[[fname]]) %>%
        rename(
          part_id_infant_tmp = Deidentified_baby_ID,
          dx_date_raw = `Diagnosis Start Date`,
          dx_code = `Diagnosis Code`,
          dx_descrip = `Diagnosis Description`,
          dx_type = `Diagnosis Type`
        )
    })
    
    icd_list <- icd_list[!sapply(icd_list, is.null)]
    
    # Neonatal defects (separate dataset)
    neonatal_path <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2022/dataset_07_2022/neonatal_defects_release.csv"
    
    if (file.exists(neonatal_path)) {
      message("[GNV] reading neonatal_defects")
      
      neonatal_df <- read_csv(neonatal_path, show_col_types = FALSE) %>%
        mutate(dx_category = "neonatal_defects") %>%
        rename(
          part_id_infant_tmp = Deidentified_baby_ID,
          dx_date_raw = `Diagnosis Start Date`,
          dx_code = `Diagnosis Code`,
          dx_descrip = `Diagnosis Description`,
          dx_type = `Diagnosis Type`
        )
      
      icd_list <- c(icd_list, list(neonatal_df))
    }
    
  } else if (site == "JAX") {
    
    fpath <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2025/dataset_04_2025/neonatal_defects_release_Jax.csv"
    
    if (!file.exists(fpath)) stop("[JAX] file not found: ", fpath)
    
    message("[JAX] reading ICD file")
    
    icd_list <- list(
      read_csv(fpath, show_col_types = FALSE) %>%
        rename(
          part_id_infant_tmp = dplyr::any_of(c("Deidentified_baby_ID", "deidentified_baby_id")),
          dx_code = dplyr::any_of(c("Diagnosis Code", "diagnosis_code")),
          dx_date_raw = dplyr::any_of(c("Diagnosis Start Date", "diagnosis_start_date")),
          dx_descrip = dplyr::any_of(c("Diagnosis Description", "diagnosis_description")),
          dx_type = dplyr::any_of(c("Diagnosis Type", "diagnosis_type"))
        ) %>%
        mutate(dx_category = "jax_icd")
    )
  }
  
  # ===============================
  # COMBINE + STANDARDIZE
  # ===============================
  
  infant_icd <- bind_rows(icd_list) %>%
    mutate(
      part_id_infant = paste0(site_id_prefix, "-infant-", part_id_infant_tmp),
      dx_date = suppressWarnings(parse_date_time(
        dx_date_raw,
        orders = c("mdy HMS", "mdy HM", "ymd HMS", "ymd HM", "mdy", "ymd")
      )),
      site = site
    ) %>%
    select(part_id_infant, dx_category, dx_code, dx_descrip, dx_date, dx_type, site)
  
  # ===============================
  # JOIN TO MOM-BABY LINK
  # ===============================
  
  if (!is.null(mom_baby_link_df)) {
    
    message("[", site, "] joining to mom_baby_link")
    
    mb <- mom_baby_link_df %>%
      select(part_id_infant, delivery_id, part_dob_infant)
    
    infant_icd <- infant_icd %>%
      left_join(mb, by = "part_id_infant") %>%
      mutate(
        dx_date = as_date(dx_date),
        part_dob_infant = as_date(part_dob_infant),
        dx2delivery_days = as.numeric(dx_date - part_dob_infant)
      ) %>%
      select(
        part_id_infant,
        delivery_id,
        dx_category,
        dx_code,
        dx_descrip,
        dx_date,
        dx2delivery_days,
        dx_type,
        site
      )
    
  } else {
    warning("[", site, "] mom_baby_link_df not provided — skipping linkage")
  }
  
  # ===============================
  # OUTPUT
  # ===============================
  
  date_tag <- format(Sys.Date(), "%Y%m%d")
  
  out_dir <- file.path(working_dir, "data", "processed", site)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  file_base <- paste0("infant_icd_", site, "_", date_tag)
  
  save(infant_icd, file = file.path(out_dir, paste0(file_base, ".rda")))
  write_csv(infant_icd, file.path(out_dir, paste0(file_base, ".csv")), na = "")
  
  message("[", site, "] rows: ", nrow(infant_icd))
  message("[", site, "] missing delivery_id: ", sum(is.na(infant_icd$delivery_id)))
  
  return(infant_icd)
}