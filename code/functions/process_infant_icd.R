process_infant_icd <- function(site, working_dir = getwd(), registry = NULL, mom_baby_link_df = NULL) {
  source(file.path(working_dir, "code/functions/nested_ingestion_mom_ehr.R"))
  registry <- registry %||% load_dataset_registry()

  components <- setdiff(names(registry$datasets$infant_icd$components), "schema_map")

  df <- purrr::map_df(components, function(comp) {
    raw <- read_standardized_dataset("infant_icd", comp, site, registry)
    raw$data %>%
      mutate(
        part_id_infant = paste0(site, "-infant-", part_id_infant_tmp),
        dx_category = comp
      ) %>%
      select(part_id_infant, dx_category, dx_date, dx_code, dx_descrip, dx_type)
  })

  if (!is.null(mom_baby_link_df)) {
    df <- df %>% left_join(mom_baby_link_df %>% select(part_id_infant, delivery_id, part_dob_infant), by = "part_id_infant") %>%
      mutate(dx2delivery_days = as.numeric(as.Date(dx_date) - as.Date(part_dob_infant)))
  }

  df <- df %>% mutate(site = site)
  df
}
