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
  order_candidates <- c("Deid-Lab Order number", "Deid-Lab Order Number")
  required <- c(raw_id_col, "Lab Name", "Lab Result", "Lab Result (#)", "Lab Unit")
  header <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE))
  missing <- setdiff(required, header)
  if (length(missing)) stop("Missing columns: ", paste(missing, collapse = ", "))
  order_col <- intersect(order_candidates, header)
  order_col <- if (length(order_col)) order_col[[1]] else NA_character_
  date_col <- intersect(c("Inferred Specimen Datetime", "Deid-Inferred Specimen Datetime"), header)
  if (!length(date_col)) stop("Missing specimen datetime column: ", path)
  date_col <- date_col[[1]]
  loinc_col <- intersect(c("Inferred LOINC Code", "LOINC Code"), header)
  loinc_desc_col <- intersect(c("Inferred LONIC Code & Desc", "Inferred LOINC Code & Desc", "LOINC Desc and Code"), header)
  loinc_col <- if (length(loinc_col)) loinc_col[[1]] else NA_character_
  loinc_desc_col <- if (length(loinc_desc_col)) loinc_desc_col[[1]] else NA_character_
  message("[LAB MAP] raw participant ", raw_id_col, " -> lab_participant_id_raw")
  message("[LAB MAP] ", date_col, " -> specimen_datetime_raw")
  message("[LAB MAP] order=", if(is.na(order_col)) "<absent; NA>" else order_col,
          "; LOINC=",if(is.na(loinc_col)) "<absent; NA>" else loinc_col,
          "; LOINC description=",if(is.na(loinc_desc_col)) "<absent; NA>" else loinc_desc_col)
  df <- readr::read_csv(path, n_max = n_max,
    col_types = readr::cols(.default = readr::col_character()),
    show_col_types = FALSE, progress = FALSE)
  n <- nrow(df)
  getcol <- function(name) if (is.na(name)) rep(NA_character_,n) else trimws(as.character(df[[name]]))
  blank_na <- function(x) { x[is.na(x) | !nzchar(x)] <- NA_character_; x }
  id_raw <- blank_na(getcol(raw_id_col))
  prefix <- if (site == "GNV") "AC" else "DC"
  role_prefix <- if (role == "mom") "mom" else "infant"
  raw_value <- blank_na(getcol("Lab Result (#)"))
  value_num <- suppressWarnings(as.numeric(raw_value))
  stamp_raw <- blank_na(getcol(date_col))
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
    loinc_code = blank_na(getcol(loinc_col)),
    loinc_description_code = blank_na(getcol(loinc_desc_col)),
    source_file = basename(path),
    source_row = seq_len(n),
    lab_link_status = "unverified_candidate",
    part_id_mom = NA_character_,
    part_id_infant = NA_character_,
    delivery_id = NA_character_,
    days_relative_delivery = NA_real_
  )
  stopifnot(nrow(out) == n, !anyNA(out$source_row),
            all(is.na(out$part_id_mom)), all(is.na(out$part_id_infant)),
            all(is.na(out$delivery_id)))
  message("[LAB QC] ", basename(path), ": ", n, " rows; ",
          sum(is.na(id_raw)), " missing IDs, ",
          sum(is.na(stamp)), " missing/unparsed times, ",
          sum(!is.na(raw_value) & is.na(value_num)), " nonnumeric numeric-field values")
  out
}
