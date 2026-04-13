process_delivery_encounter <- function(site, working_dir = getwd(), registry = NULL, mom_baby_link_df = NULL) {
  source(file.path(working_dir, "code/functions/nested_ingestion_mom_ehr.R"))
  source(file.path(working_dir, "code/functions/all_functions.R"))
  registry <- registry %||% load_dataset_registry()

  raw <- read_standardized_dataset("delivery_encounter", "raw", site, registry)

  df <- raw$data %>%
    mutate(
      part_id_mom = paste0(site, "-mom-", part_id_mom_tmp),
      part_id_infant = paste0(site, "-infant-", part_id_infant_tmp),
      part_dob_infant = parse_flexible_date(date_of_delivery),
      delivery_admit_date = parse_flexible_date(delivery_admit_date)
    ) %>%
    select(-part_id_mom_tmp, -part_id_infant_tmp)

  if (!is.null(mom_baby_link_df)) {
    df <- df %>% left_join(mom_baby_link_df %>% select(part_id_infant, delivery_id), by = "part_id_infant")
  }

  parsed <- purrr::map(df$gestational_age_infant, parse_gestational_age)
  df$gestational_age_weeks_total <- purrr::map_dbl(parsed, "weeks") + purrr::map_dbl(parsed, "days")/7

  df <- df %>%
    mutate(
      preterm_gestational_age = if_else(gestational_age_weeks_total <= 37, 1, 0),
      birth_wt_lbw = if_else(birth_weight_infant < 2500, 1, 0),
      site = site
    )

  df
}
