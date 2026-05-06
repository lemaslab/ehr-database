process_delivery_encounter <- function(site,
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
    
    fpath <- "V:/FACULTY/DJLEMAS/EHR_Data_raw/raw/READ_ONLY_DATASETS/10year_data/2021/dataset_09_2021/baby_mom_at_birth_with_payer.csv"
    
    if (!file.exists(fpath)) {
      stop("[GNV] file not found: ", fpath)
    }
    
    message("[GNV] reading: ", basename(fpath))
    
    delivery <- read_csv(fpath, show_col_types = FALSE)
    
    message("[GNV] columns:")
    print(names(delivery))
    
    delivery <- delivery %>%
      rename(
        part_id_infant_tmp   = dplyr::any_of(c("deidentified_baby_id", "Deidentified_baby_ID")),
        part_id_mom_tmp      = dplyr::any_of(c("deidentified_mom_id", "Deidentified_mom_ID")),
        delivery_date        = dplyr::any_of(c("date_of_delivery", "delivery_date", "Delivery Date")),
        birth_weight         = dplyr::any_of(c("birth_weight", "Birth Weight")),
        delivery_type        = dplyr::any_of(c("pediatric_delivery_type", "delivery_type", "Delivery Method")),
        admit_source         = dplyr::any_of(c("admit_source", "Admit Source")),
        gestational_age      = dplyr::any_of(c("pediatric_gestational_age", "gestational_age", "Gestational Age")),
        sex                  = dplyr::any_of(c("sex", "Sex")),
        race_infant          = dplyr::any_of(c("race_infant", "Race")),
        ethnicity_infant     = dplyr::any_of(c("ethnicity_infant", "Ethnicity")),
        race_mom             = dplyr::any_of(c("race_mom", "Race_mom")),
        ethnicity_mom        = dplyr::any_of(c("ethnicity_mom", "Ethnicity_mom")),
        age_at_encounter_mom = dplyr::any_of(c("age_at_encounter_mom", "Deid-Age at Encounter_mom")),
        payer_mom            = dplyr::any_of(c("payer_mom")),
        admit_date_mom       = dplyr::any_of(c("admit_date_mom", "Deid-Admit Date_mom")),
        admit_height_inches  = dplyr::any_of(c("admit_height_inches", "Admit Height (in)")),
        admit_weight_lbs     = dplyr::any_of(c("admit_weight_lbs", "Admit Weight (lbs)")),
        admit_weight_kg      = dplyr::any_of(c("admit_weight_kg", "Admit Weight (kg)")),
        admit_height_cm      = dplyr::any_of(c("admit_height_cm", "Admit Height (cm)")),
        admit_bmi            = dplyr::any_of(c("admit_bmi", "Admit BMI"))
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
    
    message("[JAX] reading: ", basename(fpath))
    
    delivery <- read_csv(fpath, show_col_types = FALSE)
    
    message("[JAX] original columns:")
    print(names(delivery))
    
    # explicit renaming
    delivery <- delivery %>%
      rename(
        part_id_infant_tmp   = Deidentified_baby_ID,
        part_id_mom_tmp      = Deidentified_mom_ID,
        delivery_date        = date_of_delivery,
        birth_weight         = birth_weight,
        delivery_type        = pediatric_delivery_type,
        admit_source         = admit_source,
        gestational_age      = pediatric_gestational_age,
        sex                  = sex,
        race_infant          = race,
        ethnicity_infant     = ethnicity,
        race_mom             = race_mom,
        ethnicity_mom        = ethnicity_mom,
        age_at_encounter_mom = age_at_encounter_mom,
        admit_date_mom       = admit_date_mom,
        admit_height_inches  = admit_height_in,
        admit_weight_lbs     = admit_weight_lbs,
        admit_weight_kg      = admit_weight_kg,
        admit_height_cm      = admit_height_cm,
        admit_bmi            = admit_bmi
      )
    
    # add missing standardized columns expected downstream
    if (!"payer_mom" %in% names(delivery)) {
      delivery <- delivery %>% mutate(payer_mom = NA_character_)
    }
    
    message("[JAX] renamed columns:")
    print(names(delivery))
  }
  
  # ===============================
  # REQUIRED CHECK
  # ===============================
  
  required_cols <- c(
    "part_id_mom_tmp",
    "part_id_infant_tmp",
    "delivery_date"
  )
  
  missing_cols <- setdiff(required_cols, names(delivery))
  
  if (length(missing_cols) > 0) {
    stop("[", site, "] Missing required columns after rename: ",
         paste(missing_cols, collapse = ", "))
  }
  
  # ===============================
  # ID STANDARDIZATION
  # ===============================
  
  site_map <- c("GNV" = "AC", "JAX" = "DC")
  site_prefix <- site_map[[site]]
  
  delivery <- delivery %>%
    mutate(
      part_id_mom = paste0(site_prefix, "-mom-", part_id_mom_tmp),
      part_id_infant_raw = paste0(site_prefix, "-infant-", part_id_infant_tmp),
      site = site
    )
  
  # ===============================
  # JOIN TO MOM-BABY LINK
  # ===============================
  
  if (!is.null(mom_baby_link_df)) {
    
    message("[", site, "] joining to mom_baby_link")
    
    delivery <- delivery %>%
      left_join(
        mom_baby_link_df %>%
          select(part_id_mom, part_id_infant, delivery_id, part_dob_infant),
        by = "part_id_mom"
      )
    
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
        orders = c("mdy HMS", "ymd HMS", "mdy HM", "ymd HM", "mdy", "ymd")
      )),
      admit_date_mom = if ("admit_date_mom" %in% names(.)) {
        suppressWarnings(parse_date_time(
          admit_date_mom,
          orders = c("mdy HMS", "ymd HMS", "mdy HM", "ymd HM", "mdy", "ymd")
        ))
      } else {
        admit_date_mom
      }
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
      birth_weight,
      admit_source,
      sex,
      race_infant,
      ethnicity_infant,
      race_mom,
      ethnicity_mom,
      age_at_encounter_mom,
      payer_mom,
      admit_date_mom,
      admit_height_inches,
      admit_weight_lbs,
      admit_weight_kg,
      admit_height_cm,
      admit_bmi,
      site,
      everything()
    ) %>%
    distinct()
  
  # ===============================
  # OUTPUT
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