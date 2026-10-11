# Derive timing only from a validated local linkage RDA and note timestamps.
add_clinical_note_timing <- function(notes, working_dir = getwd()) {
  stopifnot(is.data.frame(notes))
  suppressPackageStartupMessages(library(dplyr))
  paths <- sort(list.files(file.path(working_dir, "data", "processed", "COMBINED"),
    pattern = "^mom_baby_link_all_sites_[0-9]{8}\\.rda$", full.names = TRUE),
    decreasing = TRUE)
  if (!length(paths)) stop("Missing local canonical mom_baby_link RDA.")
  env <- new.env(parent = emptyenv())
  load(paths[1L], envir = env)
  if (!exists("mom_baby_link_all_sites", envir = env, inherits = FALSE))
    stop("Linkage RDA does not contain mom_baby_link_all_sites.")
  links <- get("mom_baby_link_all_sites", envir = env)
  required <- c("site", "delivery_id", "part_dob_infant")
  if (length(setdiff(required, names(links)))) stop("Linkage missing delivery dates.")
  delivery_dates <- links %>%
    transmute(site = as.character(site), delivery_id = as.character(delivery_id),
              birth_date = as.Date(part_dob_infant)) %>%
    group_by(site, delivery_id) %>%
    summarise(delivery_date = if (all(is.na(birth_date))) as.Date(NA)
       else min(birth_date, na.rm = TRUE),
      delivery_date_spread_days = if (all(is.na(birth_date))) NA_real_
       else as.numeric(max(birth_date, na.rm = TRUE) -
                       min(birth_date, na.rm = TRUE)), .groups = "drop")
  source_date <- intersect(c("note_created_datetime", "note_datetime",
                             "note_created_date", "note_date"), names(notes))
  raw <- if (length(source_date)) as.character(notes[[source_date[1L]]])
         else rep(NA_character_, nrow(notes))
  note_dates <- suppressWarnings(as.Date(raw, format = "%Y-%m-%d"))
  missing <- is.na(note_dates) & !is.na(raw) & nzchar(trimws(raw))
  if (any(missing)) {
    if (!requireNamespace("lubridate", quietly = TRUE))
      stop("Package lubridate is required to parse note dates.")
    note_dates[missing] <- as.Date(suppressWarnings(
      lubridate::parse_date_time(raw[missing],
        orders = c("ymd HMS", "mdy HMS", "ymd HM", "mdy HM", "ymd", "mdy"))))
  }
  n <- nrow(notes)
  notes$.note_date_internal <- note_dates
  out <- notes %>%
    left_join(delivery_dates, by = c("site", "delivery_id"),
              relationship = "many-to-one") %>%
    mutate(
      note_date = .note_date_internal,
      days_relative_delivery = as.numeric(note_date - delivery_date),
      days_before_delivery = if_else(!is.na(days_relative_delivery) &
        days_relative_delivery < 0, -days_relative_delivery, NA_real_),
      postpartum_day = if_else(!is.na(days_relative_delivery) &
        days_relative_delivery >= 0, days_relative_delivery, NA_real_),
      timing_period = case_when(
        is.na(days_relative_delivery) ~ "unknown",
        days_relative_delivery < 0 ~ "predelivery",
        days_relative_delivery == 0 ~ "delivery_day",
        TRUE ~ "postdelivery"),
      timing_window = case_when(
        is.na(days_relative_delivery) ~ "unknown",
        days_relative_delivery < -280 ~ "predelivery_gt280d",
        days_relative_delivery < -90 ~ "predelivery_91_280d",
        days_relative_delivery < 0 ~ "predelivery_1_90d",
        days_relative_delivery == 0 ~ "delivery_day",
        days_relative_delivery <= 7 ~ "postpartum_1_7d",
        days_relative_delivery <= 42 ~ "postpartum_8_42d",
        days_relative_delivery <= 90 ~ "postpartum_43_90d",
        days_relative_delivery <= 180 ~ "postpartum_91_180d",
        days_relative_delivery <= 365 ~ "postpartum_181_365d",
        TRUE ~ "postpartum_gt365d"),
      within_42d_postpartum = if_else(!is.na(days_relative_delivery),
        days_relative_delivery >= 0 & days_relative_delivery <= 42, NA),
      within_365d_postpartum = if_else(!is.na(days_relative_delivery),
        days_relative_delivery >= 0 & days_relative_delivery <= 365, NA)
    ) %>%
    select(-.note_date_internal) %>%
    mutate(
      delivery_link_confidence = case_when(
        delivery_link_status %in% c("infant_exact", "mom_delivery_date") ~ "high",
        delivery_link_status == "mom_single_episode" ~ "conditional",
        TRUE ~ "unresolved"
      ),
      timing_qc_reason = case_when(
        is.na(days_relative_delivery) ~ "missing_or_ambiguous_delivery_timing",
        participant_role == "mom" & note_period == "prenatal" &
          days_relative_delivery < -280 ~ "prenatal_more_than_280d_before_delivery",
        participant_role == "mom" & note_period == "prenatal" &
          days_relative_delivery > 0 ~ "prenatal_note_after_delivery",
        participant_role == "mom" & note_period == "delivery" &
          abs(days_relative_delivery) > 7 ~ "delivery_note_outside_7d_window",
        participant_role %in% c("baby", "infant") &
          days_relative_delivery < 0 ~ "infant_note_before_birth",
        TRUE ~ "none"
      ),
      timing_qc_status = case_when(
        timing_qc_reason == "missing_or_ambiguous_delivery_timing" ~ "unknown",
        timing_qc_reason == "none" ~ "consistent",
        TRUE ~ "review"
      )
    )
  if (nrow(out) != n) stop("Timing linkage changed note row count.")
  message("Timing computed for ", sum(!is.na(out$days_relative_delivery)),
          " of ", n, " notes.")
  if (any(out$delivery_date_spread_days > 3, na.rm = TRUE))
    warning("Delivery date spread >3 days for some canonical episodes.")
  out
}
