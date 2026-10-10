# Build aggregate, PHI-free descriptive statistics from pinned release RDA inputs,
# then render an HTML report. Requires successful 98_release_structured_and_notes.R.
# Usage: source("code/run/08_build_release_report.R") after setting EHR_RELEASE_DIR
local({
  release_dir <- Sys.getenv("EHR_RELEASE_DIR", "")
  if (!nzchar(release_dir)) stop("Set EHR_RELEASE_DIR to a validated release folder")
  manifest_path <- file.path(release_dir, "manifest.csv")
  qc_path <- file.path(release_dir, "validation_summary.csv")
  if (!all(file.exists(c(manifest_path, qc_path)))) stop("Release manifest/QC missing")
  manifest <- utils::read.csv(manifest_path, stringsAsFactors=FALSE)
  qc <- utils::read.csv(qc_path, stringsAsFactors=FALSE)
  if (!all(qc$passed)) stop("Cannot render report for failed release QC")
  read_dataset <- function(kind) {
    record <- manifest[manifest$dataset==kind, ,drop=FALSE]
    if (nrow(record)!=1L) stop("Missing/duplicate manifest component: ",kind)
    path <- record$source_file[[1]]
    if (!file.exists(path)) stop("Manifest input not found: ",path)
    if (!identical(unname(tools::md5sum(path)),record$md5[[1]]))
      stop("Input checksum changed since release validation: ",kind)
    env <- new.env(parent=emptyenv())
    objects <- load(path,envir=env)
    desired <- switch(kind,
      linkage="mom_baby_link_all_sites",
      delivery="delivery_encounter_all_sites",
      mom_icd="mom_icd_all_sites",
      infant_icd="infant_icd_all_sites",
      mom_ip="mom_ip_all_sites", mom_op="mom_op_all_sites",
      notes=if ("clinical_notes_metadata" %in% objects)
        "clinical_notes_metadata" else "clinical_notes_metadata_all_sites")
    if (!desired %in% objects) stop("Expected RDA object missing for ",kind)
    get(desired,envir=env,inherits=FALSE)
  }
  safe_unique <- function(df, name) {
    if (name %in% names(df))
      length(unique(stats::na.omit(as.character(df[[name]]))))
    else NA_integer_
  }
  safe_missing <- function(df, name) {
    if (name %in% names(df)) sum(is.na(df[[name]])) else NA_integer_
  }
  details <- list()
  site_summary <- list()
  top_categories <- list()
  temporal <- list()
  kinds <- manifest$dataset
  for (kind in kinds) {
    message("Summarizing for HTML: ", kind)
    x <- read_dataset(kind)
    details[[kind]] <- data.frame(
      dataset=kind, rows=nrow(x), columns=ncol(x),
      unique_mothers=safe_unique(x,"part_id_mom"),
      unique_infants=safe_unique(x,"part_id_infant"),
      unique_deliveries=safe_unique(x,"delivery_id"),
      unique_notes=safe_unique(x,"note_uid_global"),
      stringsAsFactors=FALSE)
    if ("site" %in% names(x)) {
      s <- as.data.frame(table(x$site,useNA="ifany"),
                         stringsAsFactors=FALSE)
      names(s) <- c("site","records")
      s$dataset <- kind
      site_summary[[kind]] <- s[,c("dataset","site","records")]
    }
    date_col <- switch(kind,delivery="delivery_date",
      mom_icd="dx_date",infant_icd="dx_date",
      mom_ip="date_med_taken",mom_op="date_med_order",
      notes=if ("note_created_datetime" %in% names(x))
        "note_created_datetime" else NA_character_,
      NA_character_)
    if (!is.na(date_col) && date_col %in% names(x)) {
      ds <- tryCatch(suppressWarnings(as.Date(x[[date_col]])),
        error=function(e) rep(as.Date(NA), nrow(x)))
      nonmiss <- ds[!is.na(ds)]
      temporal[[kind]] <- data.frame(dataset=kind,date_column=date_col,
        missing_dates=sum(is.na(ds)),min_date=if(length(nonmiss))
        as.character(min(nonmiss)) else NA_character_,
        max_date=if(length(nonmiss)) as.character(max(nonmiss)) else NA_character_,
        stringsAsFactors=FALSE)
    }
    category <- switch(kind,delivery="delivery_type",
      mom_icd=if ("dx_type" %in% names(x)) "dx_type" else NA_character_,
      infant_icd=if ("dx_category" %in% names(x)) "dx_category" else
        if ("dx_type" %in% names(x)) "dx_type" else NA_character_,
      notes="delivery_link_status",
      NA_character_)
    if (!is.na(category) && category %in% names(x)) {
      t <- sort(table(as.character(x[[category]]),useNA="ifany"),
                decreasing=TRUE)
      t <- head(t,10L)
      top_categories[[kind]] <- data.frame(dataset=kind,
        variable=category,category=names(t),
        records=as.integer(t),stringsAsFactors=FALSE)
    }
    if (kind=="notes") for (nm in c("participant_role","note_period","timing_qc_status")) {
      if (nm %in% names(x)) {
        t <- sort(table(as.character(x[[nm]]), useNA="ifany"),decreasing=TRUE)
        top_categories[[paste(kind,nm,sep="_")]] <- data.frame(
          dataset=kind,variable=nm,category=names(t),
          records=as.integer(t),stringsAsFactors=FALSE)
      }
    }
    rm(x); gc(verbose=FALSE)
  }
  join_rows <- function(x) {
    if (length(x)) do.call(rbind,unname(x)) else data.frame()
  }
  utils::write.csv(join_rows(details),file.path(release_dir,"dataset_overview.csv"),row.names=FALSE)
  utils::write.csv(join_rows(site_summary),file.path(release_dir,"site_summary.csv"),row.names=FALSE)
  utils::write.csv(join_rows(temporal),file.path(release_dir,"date_summary.csv"),row.names=FALSE)
  utils::write.csv(join_rows(top_categories),file.path(release_dir,"category_summary.csv"),row.names=FALSE)
  # Reviewable raw-source provenance (paths and file metadata, no patient rows).
  source("code/run/00_raw_source_inventory.R", local=TRUE)
  ehr_raw_source_inventory(root=getwd(),output_dir=release_dir,
    hash_files=tolower(Sys.getenv("EHR_RAW_SOURCE_HASH","false")) %in% c("true","1","yes"))
  # Network verification is a separate, non-blocking release audit.
  # The historical validated Oct 10 release predates standardized network
  # exports for linkage/delivery, so missing files are explicitly reported.
  net_base <- Sys.getenv("EHR_NETWORK_BASE",
    "V:/FACULTY/DJLEMAS/EHR_Data_processed")
  network_qc <- do.call(rbind,lapply(seq_len(nrow(manifest)),function(i) {
    src <- manifest$source_file[i]
    dataset <- manifest$dataset[i]
    relative <- file.path("COMBINED",basename(src))
    dst <- file.path(net_base,relative)
    present <- file.exists(dst)
    match <- if(present) identical(unname(tools::md5sum(dst)),manifest$md5[i]) else FALSE
    csv <- sub("\\\\.rda$",".csv",dst,ignore.case=TRUE)
    data.frame(dataset=dataset,network_rda=dst,rda_exists=present,
      rda_md5_matches=match,network_csv=csv,
      csv_exists=file.exists(csv),
      csv_nonempty=if(file.exists(csv))isTRUE(file.info(csv)$size>0) else FALSE,
      stringsAsFactors=FALSE)
  }))
  utils::write.csv(network_qc,file.path(release_dir,"network_verification.csv"),row.names=FALSE)
  message("Network audit: RDA MD5 matched ",
    sum(network_qc$rda_md5_matches),"/",nrow(network_qc),
    "; network CSV present ",sum(network_qc$csv_exists),"/",nrow(network_qc))
  if (!requireNamespace("rmarkdown",quietly=TRUE)) {
    warning("Install rmarkdown to render HTML; aggregate CSVs are ready.")
  } else {
    report <- file.path(getwd(),"reports","ehr_release_report.Rmd")
    if (!file.exists(report)) stop("HTML report template missing: ",report)
    tryCatch({
      rmarkdown::render(report,
        output_file="ehr_release_report.html",output_dir=release_dir,
        params=list(release_dir=normalizePath(release_dir,winslash="/")),
        envir=new.env(parent=globalenv()),quiet=TRUE)
      message("HTML release report: ",file.path(release_dir,"ehr_release_report.html"))
    },error=function(e) warning("HTML render failed; CSV summaries remain available: ",
                               conditionMessage(e)))
  }
})
