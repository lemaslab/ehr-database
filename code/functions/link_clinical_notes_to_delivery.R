# Attach validated delivery episodes to clinical-notes metadata.
# Offline: reads only a local combined linkage RDA. No remote lookups.
link_clinical_notes_to_delivery <- function(notes, working_dir = getwd()) {
  stopifnot(is.data.frame(notes))
  require_cols <- c("site", "participant_role", "note_period",
                    "part_id_mom", "part_id_infant")
  if (length(setdiff(require_cols, names(notes))))
    stop("Clinical notes missing: ", paste(setdiff(require_cols, names(notes)), collapse = ", "))

  input_dir <- file.path(working_dir, "data", "processed", "COMBINED")
  candidates <- list.files(input_dir,
    pattern = "^mom_baby_link_all_sites_[0-9]{8}\\.rda$", full.names = TRUE)
  if (!length(candidates)) stop(
    "Missing local mom_baby_link_all_sites_YYYYMMDD.rda in ", input_dir,
    ". Supply a validated local RDA; this stage will not rebuild linkage."
  )
  path <- sort(candidates, decreasing = TRUE)[1L]
  env <- new.env(parent = emptyenv())
  load(path, envir = env)
  if (!exists("mom_baby_link_all_sites", env, inherits = FALSE))
    stop("Linkage RDA does not contain mom_baby_link_all_sites: ", path)
  link <- get("mom_baby_link_all_sites", env)
  required <- c("site", "part_id_mom", "part_id_infant", "delivery_id",
                "part_dob_infant")
  if (length(setdiff(required, names(link))))
    stop("Linkage RDA missing: ", paste(setdiff(required, names(link)), collapse = ", "))
  suppressPackageStartupMessages(library(dplyr))
  link <- link %>%
    transmute(site = as.character(site),
              part_id_mom = as.character(part_id_mom),
              part_id_infant = as.character(part_id_infant),
              delivery_id = as.character(delivery_id),
              delivery_date = as.Date(part_dob_infant)) %>%
    distinct()
  if (anyNA(link$part_id_infant) || anyNA(link$delivery_id) ||
      anyDuplicated(link[c("site", "part_id_infant")]))
    stop("Canonical linkage requires unique site + infant and nonmissing delivery IDs.")

  n_before <- nrow(notes)
  notes$.link_row_internal <- seq_len(n_before)
  infants <- notes %>%
    filter(participant_role %in% c("baby", "infant"),
           !is.na(part_id_infant), part_id_infant != "") %>%
    select(.link_row_internal, site, part_id_infant) %>%
    left_join(select(link, site, part_id_infant, delivery_id),
              by = c("site", "part_id_infant"), relationship = "many-to-one") %>%
    filter(!is.na(delivery_id)) %>%
    transmute(.link_row_internal, delivery_id,
              delivery_link_status = "infant_exact")

  episodes <- link %>%
    filter(!is.na(part_id_mom), part_id_mom != "") %>%
    group_by(site, part_id_mom, delivery_id) %>%
    summarise(delivery_date = if (all(is.na(delivery_date))) as.Date(NA)
              else min(delivery_date, na.rm = TRUE), .groups = "drop")
  counts <- episodes %>%
    count(site, part_id_mom, name = "n_delivery_episodes")
  singleton <- episodes %>%
    inner_join(filter(counts, n_delivery_episodes == 1L),
               by = c("site", "part_id_mom")) %>%
    select(site, part_id_mom, delivery_id)
  maternal_notes <- notes %>%
    filter(participant_role == "mom", !is.na(part_id_mom),
           part_id_mom != "") %>%
    select(.link_row_internal, site, part_id_mom, note_period,
           any_of("note_created_datetime"))
  single_matches <- maternal_notes %>%
    left_join(singleton, by = c("site", "part_id_mom"),
              relationship = "many-to-one") %>%
    filter(!is.na(delivery_id)) %>%
    transmute(.link_row_internal, delivery_id,
              delivery_link_status = "mom_single_episode")

  # Mothers with multiple deliveries: only date-confirmed delivery-period notes.
  multi <- maternal_notes %>%
    anti_join(select(single_matches, .link_row_internal),
              by = ".link_row_internal") %>%
    filter(note_period == "delivery") %>%
    inner_join(filter(counts, n_delivery_episodes > 1L),
               by = c("site", "part_id_mom"))
  if ("note_created_datetime" %in% names(multi)) {
    raw <- as.character(multi$note_created_datetime)
    dates <- suppressWarnings(as.Date(raw))
    need_parse <- is.na(dates) & !is.na(raw) & nzchar(raw)
    if (any(need_parse)) {
      if (!requireNamespace("lubridate", quietly = TRUE))
        stop("lubridate required for note date parsing")
      dates[need_parse] <- as.Date(suppressWarnings(
        lubridate::parse_date_time(raw[need_parse],
          orders = c("ymd HMS", "mdy HMS", "ymd HM", "mdy HM", "ymd", "mdy"))))
    }
    multi$note_link_date <- dates
  } else {
    multi$note_link_date <- as.Date(NA)
  }
  date_matches <- multi %>%
    filter(!is.na(note_link_date)) %>%
    select(.link_row_internal, site, part_id_mom, note_link_date) %>%
    inner_join(episodes, by = c("site", "part_id_mom"),
               relationship = "many-to-many") %>%
    filter(!is.na(delivery_date),
           abs(as.numeric(note_link_date - delivery_date)) <= 3) %>%
    group_by(.link_row_internal) %>%
    filter(n_distinct(delivery_id) == 1L) %>%
    ungroup() %>%
    distinct(.link_row_internal, delivery_id) %>%
    mutate(delivery_link_status = "mom_delivery_date")

  matches <- bind_rows(infants, single_matches, date_matches)
  if (anyDuplicated(matches$.link_row_internal))
    stop("Delivery linkage duplicated a note; refusing to save.")
  output <- notes %>%
    left_join(matches, by = ".link_row_internal",
              relationship = "one-to-one") %>%
    left_join(counts, by = c("site", "part_id_mom"),
              relationship = "many-to-one") %>%
    mutate(
      delivery_link_status = case_when(
        !is.na(delivery_id) ~ delivery_link_status,
        participant_role == "mom" & coalesce(n_delivery_episodes, 0L) > 1L ~
          "ambiguous_mom_episodes",
        TRUE ~ "unmatched"),
      delivery_episode_key = delivery_id
    ) %>%
    select(-.link_row_internal, -n_delivery_episodes)
  if (nrow(output) != n_before) stop("Delivery linkage changed note count.")
  message("Local linkage source: ", path)
  print(output %>%
    count(site, participant_role, delivery_link_status, name = "n_notes"))
  output
}
