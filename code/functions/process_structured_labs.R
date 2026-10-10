# Stage 07: standardize one maternal or infant lab CSV, preserving all source rows.
# The nine-digit source IDs are not validated against canonical delivery linkage.
process_structured_labs <- function(path, site, role, n_max = Inf) {
  if (!site %in% c("GNV", "JAX") || !role %in% c("mom", "baby"))
    stop("Invalid lab site or role.")
  stopifnot(file.exists(path))
  if (!requireNamespace("readr", quietly = TRUE) ||
      !requireNamespace("dplyr", quietly = TRUE))
    stop("Stage 07 requires readr and dplyr.")
  raw_id_col <- if (role == "mom") "Deidentified_mom_ID" else "Deidentified_baby_ID"
  order_col <- if (role == "mom") "Deid-Lab Order number" else "Deid-Lab Order Number"
  required <- c(raw_id_col, order_col, "Lab Name", "Lab Result",
                "Lab Result (#)", "Lab Unit", "Deid-Inferred Specimen Datetime")
  header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE))
  missing <- setdiff(required, header)
  if (length(missing)) stop("Missing columns: ", paste(missing, collapse = ", "))
  if (role == "baby" && !all(c("LOINC Code", "LOINC Desc and Code") %in% header))
    stop("Infant LOINC columns missing: ", path)
  df <- readr::read_csv(path, n_max = n_max,
    col_types = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE, progress = FALSE)
  n <- nrow(df)
  getcol <- function(name) trimws(as.character(df[[name]]))
  blank_na <- function(x) { x[is.na(x) | !nzchar(x)] <- NA_character_; x }
  id_raw <- blank_na(getcol(raw_id_col))
  prefix <- if (site == "GNV") "AC" else "DC"
  role_prefix <- if (role == "mom") "mom" else "infant"
  raw_value <- blank_na(getcol("Lab Result (#)"))
  value_num <- suppressWarnings(as.numeric(raw_value))
  stamp_raw <- blank_na(getcol("Deid-Inferred Specimen Datetime"))
  stamp <- as.POSIXct(stamp_raw, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
  out <- dplyr::tibble(
    site = site,
    participant_role = role,
    lab_participant_id_raw = id_raw,
    lab_participant_id = ifelse(is.na(id_raw), NA_character_,
                                paste0(prefix, "-", role_prefix, "-", id_raw)),
    lab_order_id = blank_na(getcol(order_col)),
    lab_name = blank_na(getcol("Lab Name")),
    lab_result_raw = blank_na(getcol("Lab Result")),
    lab_result_numeric_raw = raw_value,
    lab_result_numeric = value_num,
    lab_unit = blank_na(getcol("Lab Unit")),
    specimen_datetime_raw = stamp_raw,
    specimen_datetime = stamp,
    loinc_code = if ("LOINC Code" %in% header) blank_na(getcol("LOINC Code"))
                 else rep(NA_character_, n),
    loinc_description_code = if ("LOINC Desc and Code" %in% header)
       blank_na(getcol("LOINC Desc and Code")) else rep(NA_character_, n),
    source_file = basename(path),
    source_row = seq_len(n),
    lab_link_status = "unresolved_namespace",
    part_id_mom = NA_character_,
    part_id_infant = NA_character_,
    delivery_id = NA_character_,
    days_relative_delivery = NA_real_
  )
  stopifnot(nrow(out) == n, !anyNA(out$source_row),
            all(is.na(out$part_id_mom)), all(is.na(out$part_id_infant)),
            all(is.na(out$delivery_id)))
  message(basename(path), ": ", n, " rows; ",
          sum(is.na(id_raw)), " missing IDs, ",
          sum(is.na(stamp)), " missing/unparsed times, ",
          sum(!is.na(raw_value) & is.na(value_num)), " nonnumeric numeric-field values")
  out
}
