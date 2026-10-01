process_apgar <- function(site, working_dir = getwd(), registry = NULL, mom_baby_link_df = NULL) {
  source(file.path(working_dir, "code/functions/nested_ingestion_mom_ehr.R"))
  registry <- registry %||% load_dataset_registry()

  raw <- read_standardized_dataset("apgar", "raw", site, registry)

  df <- raw$data %>%
    mutate(
      part_id_infant = paste0(site, "-infant-", part_id_infant_tmp),
      apgar5min_score_low = if_else(apgar_total_5min <= 3, 1, 0)
    )

  if (!is.null(mom_baby_link_df)) {
    df <- df %>% left_join(mom_baby_link_df %>% select(part_id_infant, delivery_id), by = "part_id_infant")
  }

  df <- df %>% mutate(site = site)
  df
}
