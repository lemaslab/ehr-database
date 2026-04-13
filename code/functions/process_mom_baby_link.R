process_mom_baby_link <- function(site, working_dir = getwd(), registry = NULL) {
  source(file.path(working_dir, "code/functions/nested_ingestion_mom_ehr.R"))
  registry <- registry %||% load_dataset_registry()

  linkage <- read_standardized_dataset("mom_baby_link", "linkage", site, registry)
  birth   <- read_standardized_dataset("mom_baby_link", "birth", site, registry)

  df_link <- linkage$data %>%
    mutate(
      part_id_mom = paste0(site, "-mom-", part_id_mom_tmp),
      part_id_infant = paste0(site, "-infant-", part_id_infant_tmp)
    ) %>%
    select(-part_id_mom_tmp, -part_id_infant_tmp)

  df_birth <- birth$data %>%
    mutate(
      part_id_infant = paste0(site, "-infant-", part_id_infant_tmp),
      part_dob_infant = parse_flexible_datetime(part_dob_infant_raw)
    ) %>%
    select(part_id_infant, part_dob_infant)

  df <- df_link %>%
    left_join(df_birth, by = "part_id_infant") %>%
    arrange(part_id_mom, part_dob_infant) %>%
    group_by(part_id_mom) %>%
    mutate(new_delivery = if_else(is.na(lag(part_dob_infant)) |
                                   as.numeric(part_dob_infant - lag(part_dob_infant)) > 3, 1, 0),
           delivery_id = paste0("delivery-", cumsum(new_delivery))) %>%
    ungroup() %>%
    mutate(site = site) %>%
    select(part_id_mom, part_id_infant, delivery_id, part_dob_infant, site)

  df
}
