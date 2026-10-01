# Stage 03: deep linkage QC
#
# Audits the canonical AC/DC mother-infant-delivery linkage without changing it.

source("code/run/00_pipeline_helpers.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
})

working_dir <- getwd()

gnv_link <- readRDS(cache_path("GNV", "mom_baby_link", working_dir))
jax_link <- readRDS(cache_path("JAX", "mom_baby_link", working_dir))
gnv_delivery <- readRDS(cache_path("GNV", "delivery_encounter", working_dir))
jax_delivery <- readRDS(cache_path("JAX", "delivery_encounter", working_dir))

link <- bind_rows(gnv_link, jax_link)
delivery <- bind_rows(gnv_delivery, jax_delivery)

# 1. Infant integrity ---------------------------------------------------------
infant_qc <- link %>%
  group_by(part_id_infant) %>%
  summarise(
    n_moms = n_distinct(part_id_mom),
    n_delivery_ids = n_distinct(delivery_id),
    n_dobs = n_distinct(part_dob_infant),
    .groups = "drop"
  )

bad_infants <- infant_qc %>%
  filter(n_moms != 1L | n_delivery_ids != 1L | n_dobs != 1L)

if (nrow(bad_infants) > 0L) {
  stop("Stage 03 QC failed: ", nrow(bad_infants),
       " infant(s) have contradictory mother/delivery/DOB linkage.")
}

# 2. Delivery episode integrity ----------------------------------------------
delivery_qc <- link %>%
  group_by(delivery_id) %>%
  summarise(
    site = first(site),
    n_moms = n_distinct(part_id_mom),
    n_infants = n_distinct(part_id_infant),
    min_birth = min(part_dob_infant, na.rm = TRUE),
    max_birth = max(part_dob_infant, na.rm = TRUE),
    birth_span_hours = as.numeric(difftime(max_birth, min_birth, units = "hours")),
    .groups = "drop"
  )

bad_delivery_moms <- delivery_qc %>% filter(n_moms != 1L)
if (nrow(bad_delivery_moms) > 0L) {
  stop("Stage 03 QC failed: ", nrow(bad_delivery_moms),
       " delivery episode(s) map to multiple mothers.")
}

delivery_qc <- delivery_qc %>%
  mutate(
    is_multiple_birth = n_infants > 1L,
    unusual_multiple_birth_span = is_multiple_birth & birth_span_hours > 2
  )

multiple_births <- delivery_qc %>% filter(is_multiple_birth)

multiple_birth_review <- delivery_qc %>%
  filter(unusual_multiple_birth_span) %>%
  arrange(desc(birth_span_hours))

multiple_birth_distribution <- delivery_qc %>%
  count(n_infants, name = "n_deliveries") %>%
  arrange(n_infants)

multiple_birth_span_summary <- multiple_births %>%
  summarise(
    multi_delivery_episodes = n(),
    within_2h = sum(birth_span_hours <= 2, na.rm = TRUE),
    over_2h = sum(birth_span_hours > 2, na.rm = TRUE),
    over_6h = sum(birth_span_hours > 6, na.rm = TRUE),
    over_24h = sum(birth_span_hours > 24, na.rm = TRUE),
    max_span_hours = max(birth_span_hours, na.rm = TRUE)
  )

# 3. Repeat pregnancies -------------------------------------------------------
mother_qc <- link %>%
  group_by(part_id_mom) %>%
  summarise(
    site = first(site),
    n_delivery_ids = n_distinct(delivery_id),
    n_infants = n_distinct(part_id_infant),
    .groups = "drop"
  )

repeat_pregnancy_summary <- mother_qc %>%
  count(n_delivery_ids, name = "n_mothers") %>%
  arrange(n_delivery_ids)

# 4. Delivery row / linkage agreement ----------------------------------------
delivery_link_qc <- delivery %>%
  mutate(
    delivery_date_parsed = as.POSIXct(delivery_date, tz = "UTC"),
    dob_parsed = as.POSIXct(part_dob_infant, tz = "UTC"),
    delivery_dob_diff_hours = as.numeric(
      difftime(delivery_date_parsed, dob_parsed, units = "hours")
    )
  )

delivery_date_summary <- delivery_link_qc %>%
  summarise(
    rows = n(),
    infants = n_distinct(part_id_infant),
    delivery_ids = n_distinct(delivery_id),
    missing_delivery_date = sum(is.na(delivery_date_parsed)),
    missing_dob = sum(is.na(dob_parsed)),
    exact_datetime = sum(delivery_dob_diff_hours == 0, na.rm = TRUE),
    within_24h = sum(abs(delivery_dob_diff_hours) <= 24, na.rm = TRUE),
    over_24h = sum(abs(delivery_dob_diff_hours) > 24, na.rm = TRUE)
  )

# 5. Maternal admission timing ------------------------------------------------
admission_qc <- delivery_link_qc %>%
  mutate(
    admit_date_parsed = as.POSIXct(admit_date_mom, tz = "UTC"),
    admit2delivery_days = as.numeric(
      difftime(admit_date_parsed, dob_parsed, units = "days")
    ),
    unusual_admit_timing = is.na(admit2delivery_days) |
      admit2delivery_days < -30 |
      admit2delivery_days > 0
  )

admission_review <- admission_qc %>%
  filter(unusual_admit_timing) %>%
  select(
    site, part_id_mom, part_id_infant, delivery_id,
    admit_date_mom, delivery_date, part_dob_infant,
    admit2delivery_days, unusual_admit_timing
  ) %>%
  arrange(site, admit2delivery_days)

admission_summary <- admission_qc %>%
  group_by(site) %>%
  summarise(
    rows = n(),
    missing_admit = sum(is.na(admit_date_parsed)),
    unusual_admit_timing = sum(unusual_admit_timing),
    admit_over_30d_before = sum(admit2delivery_days < -30, na.rm = TRUE),
    admit_after_delivery = sum(admit2delivery_days > 0, na.rm = TRUE),
    min_days = min(admit2delivery_days, na.rm = TRUE),
    max_days = max(admit2delivery_days, na.rm = TRUE),
    .groups = "drop"
  )

# 6. Explicit Cartesian-expansion check --------------------------------------
if (nrow(delivery) != n_distinct(delivery$part_id_infant)) {
  stop("Stage 03 QC failed: delivery encounter contains multiple rows per infant.")
}

if (nrow(delivery) != nrow(link)) {
  stop("Stage 03 QC failed: delivery encounter row count differs from canonical infant linkage.")
}

# Save audit object for reproducibility.
linkage_qc_report <- list(
  generated_at = Sys.time(),
  infant_qc = infant_qc,
  delivery_qc = delivery_qc,
  multiple_birth_distribution = multiple_birth_distribution,
  multiple_birth_span_summary = multiple_birth_span_summary,
  multiple_birth_review = multiple_birth_review,
  mother_qc = mother_qc,
  repeat_pregnancy_summary = repeat_pregnancy_summary,
  delivery_date_summary = delivery_date_summary,
  admission_summary = admission_summary,
  admission_review = admission_review
)

qc_dir <- file.path(working_dir, "data", "processed", "COMBINED", "qc")
dir.create(qc_dir, recursive = TRUE, showWarnings = FALSE)
qc_path <- file.path(
  qc_dir,
  paste0("linkage_qc_", format(Sys.Date(), "%Y%m%d"), ".rds")
)
atomic_save_rds(linkage_qc_report, qc_path)

# Human-reviewable exception tables. These are QC artifacts only; they do not
# alter the canonical delivery IDs or remove records from the cohort.
multiple_review_path <- file.path(
  qc_dir,
  paste0("multiple_birth_span_review_", format(Sys.Date(), "%Y%m%d"), ".csv")
)
admission_review_path <- file.path(
  qc_dir,
  paste0("admission_timing_review_", format(Sys.Date(), "%Y%m%d"), ".csv")
)

write.csv(multiple_birth_review, multiple_review_path, row.names = FALSE, na = "")
write.csv(admission_review, admission_review_path, row.names = FALSE, na = "")

cat("\n==== LINKAGE QC SUMMARY ====\n")
cat("Infants:", n_distinct(link$part_id_infant), "\n")
cat("Mothers:", n_distinct(link$part_id_mom), "\n")
cat("Delivery episodes:", n_distinct(link$delivery_id), "\n")
cat("Bad infant linkages:", nrow(bad_infants), "\n")
cat("Delivery episodes crossing mothers:", nrow(bad_delivery_moms), "\n")
cat("Delivery encounter rows:", nrow(delivery), "\n\n")

cat("Multiple-birth distribution:\n")
print(multiple_birth_distribution)

cat("\nMultiple-birth time spans:\n")
print(multiple_birth_span_summary)

cat("\nRepeat-pregnancy distribution:\n")
print(repeat_pregnancy_summary)

cat("\nDelivery date agreement:\n")
print(delivery_date_summary)

cat("\nAdmission timing:\n")
print(admission_summary)

message("[output] ", qc_path)
message("[output] ", multiple_review_path)
message("[output] ", admission_review_path)
message("Stage 03 linkage QC complete.")
