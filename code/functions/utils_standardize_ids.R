# ===============================
# Standardize participant IDs
# ===============================
#
# Final output column names remain consistent across sites:
#   - part_id_mom
#   - part_id_infant
#
# Site and participant role are encoded in the ID value, not the column name.
# Examples:
#   - AC-mom-1
#   - AC-infant-1
#   - DC-mom-1
#   - DC-infant-1

standardize_participant_ids <- function(df,
                                        site,
                                        mom_id_col = NULL,
                                        infant_id_col = NULL) {
  
  suppressPackageStartupMessages({
    library(dplyr)
  })
  
  if (is.null(mom_id_col) && is.null(infant_id_col)) {
    stop("Must provide mom_id_col and/or infant_id_col")
  }
  
  # -----------------------------
  # Site prefix mapping
  # -----------------------------
  site_prefix <- case_when(
    site %in% c("GNV", "AC", "Alachua", "Alachua County") ~ "AC",
    site %in% c("JAX", "DC", "Duval", "Duval County") ~ "DC",
    TRUE ~ site
  )
  
  # -----------------------------
  # MOM ID
  # Final output column: part_id_mom
  # -----------------------------
  if (!is.null(mom_id_col) && mom_id_col %in% names(df)) {
    df <- df %>%
      mutate(
        part_id_mom = ifelse(
          !is.na(.data[[mom_id_col]]) &
            trimws(as.character(.data[[mom_id_col]])) != "",
          paste0(site_prefix, "-mom-", trimws(as.character(.data[[mom_id_col]]))),
          NA_character_
        )
      )
  }
  
  # -----------------------------
  # INFANT ID
  # Final output column: part_id_infant
  # -----------------------------
  if (!is.null(infant_id_col) && infant_id_col %in% names(df)) {
    df <- df %>%
      mutate(
        part_id_infant = ifelse(
          !is.na(.data[[infant_id_col]]) &
            trimws(as.character(.data[[infant_id_col]])) != "",
          paste0(site_prefix, "-infant-", trimws(as.character(.data[[infant_id_col]]))),
          NA_character_
        )
      )
  }
  
  return(df)
}
