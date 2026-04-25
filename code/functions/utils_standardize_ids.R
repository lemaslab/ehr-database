# ===============================
# Standardize participant IDs
# ===============================
standardize_participant_ids <- function(df,
                                        site,
                                        mom_id_col = NULL,
                                        baby_id_col = NULL) {
  
  suppressPackageStartupMessages({
    library(dplyr)
  })
  
  if (is.null(mom_id_col) && is.null(baby_id_col)) {
    stop("Must provide mom_id_col and/or baby_id_col")
  }
  
  # -----------------------------
  # Site prefix mapping
  # -----------------------------
  site_prefix <- case_when(
    site == "GNV" ~ "AC",
    site == "JAX" ~ "DC",
    TRUE ~ site
  )
  
  # -----------------------------
  # MOM ID
  # -----------------------------
  if (!is.null(mom_id_col) && mom_id_col %in% names(df)) {
    df <- df %>%
      mutate(
        part_id_mom = ifelse(
          !is.na(.data[[mom_id_col]]) & .data[[mom_id_col]] != "",
          paste0(site_prefix, "-mom-", trimws(.data[[mom_id_col]])),
          NA_character_
        )
      )
  }
  
  # -----------------------------
  # BABY ID
  # -----------------------------
  if (!is.null(baby_id_col) && baby_id_col %in% names(df)) {
    df <- df %>%
      mutate(
        part_id_baby = ifelse(
          !is.na(.data[[baby_id_col]]) & .data[[baby_id_col]] != "",
          paste0(site_prefix, "-baby-", trimws(.data[[baby_id_col]])),
          NA_character_
        )
      )
  }
  
  return(df)
}