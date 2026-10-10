# Validate and register local structured-data + clinical-notes metadata release.
# No raw reprocessing, no maternal-to-delivery expansion, no lab integration.
# Run from repository root: source("code/run/98_release_structured_and_notes.R")
local({
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Requires dplyr")
  if (!file.exists("code/run/01_build_id_linkage.R")) stop("Run from ehr-database root")
  if (tolower(Sys.getenv("EHR_RELEASE_REBUILD", "false")) %in% c("true","1","yes"))
    stop("Rebuild disabled: missing medication runners and risk of overwriting existing RDAs.")
  root <- getwd()
  combined <- file.path(root, "data", "processed", "COMBINED")
  baseline <- Sys.getenv("EHR_RELEASE_STRUCTURED_DATE", "20261009")
  note_file <- Sys.getenv("EHR_RELEASE_METADATA_FILE",
    "clinical_notes_metadata_all_sites_computed_20261010_014429.rda")
  if (!grepl("^[A-Za-z]:|^[/\\\\]", note_file))
    note_file <- file.path(combined, note_file)
  stems <- c(linkage="mom_baby_link_all_sites",
             delivery="delivery_encounter_all_sites",
             mom_icd="mom_icd_all_sites",
             infant_icd="infant_icd_all_sites",
             mom_ip="mom_ip_all_sites",
             mom_op="mom_op_all_sites")
  paths <- setNames(file.path(combined,paste0(stems,"_",baseline,".rda")),names(stems))
  paths <- c(paths,notes=note_file)
  if (any(!file.exists(paths))) stop("Missing RDA: ", paste(paths[!file.exists(paths)],collapse="; "))
  load_one <- function(file,obj) {
    env <- new.env(parent=emptyenv())
    actual <- load(unname(file),envir=env)
    if (!obj %in% actual) stop("Object '",obj,"' missing in ",file)
    x <- get(obj,envir=env,inherits=FALSE)
    if (!is.data.frame(x)) stop("Not tabular: ",file)
    x
  }
  checks <- list()
  check <- function(dataset,test,value,ok) {
    checks[[length(checks)+1L]] <<- data.frame(dataset=dataset,test=test,
      value=as.character(value),passed=isTRUE(ok),stringsAsFactors=FALSE)
  }
  reference <- load_one(paths[["linkage"]], stems[["linkage"]])
  required <- c("site","part_id_mom","part_id_infant","delivery_id")
  if (!all(required %in% names(reference))) stop("Linkage missing required keys")
  if (anyNA(reference[,required])) stop("Canonical linkage contains missing keys")
  check("linkage","unique infant rows",nrow(reference),
        !anyDuplicated(reference$part_id_infant))
  keys <- lapply(reference[,c("part_id_mom","part_id_infant","delivery_id")],
                 function(x) unique(as.character(x)))
  relation_key <- paste(reference$site,reference$part_id_infant,sep="||")
  if (anyDuplicated(relation_key)) stop("Canonical linkage duplicates site/infant key")
  dates <- c(delivery="delivery_date", mom_icd="dx_date", infant_icd="dx_date",
             mom_ip="date_med_taken",mom_op="date_med_order")
  for (kind in names(stems)[-1L]) {
    message("Validating ",kind)
    df <- load_one(paths[[kind]],stems[[kind]])
    need <- c("site",dates[[kind]])
    if (kind %in% c("mom_icd","mom_ip","mom_op")) need <- c(need,"part_id_mom")
    if (kind %in% c("delivery","infant_icd")) need <- c(need,"part_id_infant","delivery_id")
    if (kind=="delivery") need <- c(need,"part_id_mom")
    if (!all(need %in% names(df))) stop(kind," missing: ",paste(setdiff(need,names(df)),collapse=", "))
    check(kind,"nonempty rows",nrow(df),nrow(df)>0L)
    check(kind,"valid site",sum(is.na(df$site)|!df$site %in% c("GNV","JAX")),
          all(!is.na(df$site)&df$site %in% c("GNV","JAX")))
    for (id in intersect(c("part_id_mom","part_id_infant","delivery_id"),need)) {
      x <- as.character(df[[id]])
      absent <- sum(is.na(x)|!nzchar(trimws(x)))
      unmatched <- sum(!unique(x[!is.na(x)&nzchar(trimws(x))]) %in% keys[[id]])
      check(kind,paste0(id," missing rows"),absent,absent==0L)
      check(kind,paste0(id," unmatched unique"),unmatched,unmatched==0L)
      if (id!="delivery_id") {
        pattern <- ifelse(df$site=="GNV","AC-", "DC-")
        pattern <- paste0(pattern,if (id=="part_id_mom") "mom-" else "infant-")
        bad <- sum(!is.na(x)&!startsWith(x,pattern))
        check(kind,paste0(id," prefix mismatch"),bad,bad==0L)
      }
    }
    d <- as.Date(df[[dates[[kind]]]])
    check(kind,"missing event dates",sum(is.na(d)),!anyNA(d))
    if (kind %in% c("delivery","infant_icd")) {
      pos <- match(paste(df$site,df$part_id_infant,sep="||"),relation_key)
      missing <- sum(is.na(pos))
      mismatch <- sum(!is.na(pos)&as.character(df$delivery_id)!=
                      as.character(reference$delivery_id[pos]),na.rm=TRUE)
      check(kind,"infant missing canonical relation",missing,missing==0L)
      check(kind,"infant/delivery mismatch",mismatch,mismatch==0L)
      if (kind=="delivery") {
        mom_mismatch <- sum(!is.na(pos)&as.character(df$part_id_mom)!=
                            as.character(reference$part_id_mom[pos]),na.rm=TRUE)
        check(kind,"infant/mother mismatch",mom_mismatch,mom_mismatch==0L)
        check(kind,"unique infant rows",nrow(df),!anyDuplicated(df$part_id_infant))
      }
    }
    rm(df); gc(verbose=FALSE)
  }
  rm(reference); gc(verbose=FALSE)
  message("Validating clinical notes metadata; this may consume substantial RAM.")
  env <- new.env(parent=emptyenv())
  available <- load(unname(paths[["notes"]]),envir=env)
  obj <- intersect(c("clinical_notes_metadata","clinical_notes_metadata_all_sites"), available)
  if (!length(obj)) stop("Notes metadata RDA missing expected object")
  notes <- get(obj[1L],envir=env,inherits=FALSE)
  if (!is.data.frame(notes)) stop("Notes object is not a data frame")
  check("notes","expected row count",nrow(notes),nrow(notes)==12702906L)
  note_cols <- c("note_uid_global","site","participant_role",
                 "delivery_link_status","timing_qc_status")
  if (!all(note_cols %in% names(notes))) stop("Notes metadata missing: ",
    paste(setdiff(note_cols,names(notes)),collapse=", "))
  missing_note_ids <- sum(is.na(notes$note_uid_global) |
                          !nzchar(as.character(notes$note_uid_global)))
  check("notes","missing global note IDs",missing_note_ids,missing_note_ids==0L)
  duplicates <- anyDuplicated(notes$note_uid_global)
  check("notes","duplicate global note IDs",duplicates,duplicates==0L)
  check("notes","missing timing QC",sum(is.na(notes$timing_qc_status)),
        !anyNA(notes$timing_qc_status))
  rm(notes,env);gc(verbose=FALSE)
  qc <- dplyr::bind_rows(checks)
  run_id <- format(Sys.time(),"%Y%m%d_%H%M%S")
  out <- file.path(root,"data","processed","RELEASES",paste0("ehr_release_",run_id))
  dir.create(out,recursive=TRUE,showWarnings=FALSE)
  manifest <- data.frame(dataset=names(paths),
    source_file=normalizePath(unname(paths),winslash="/",mustWork=TRUE),
    bytes=as.numeric(file.info(unname(paths))$size),
    md5=unname(tools::md5sum(unname(paths))),stringsAsFactors=FALSE)
  utils::write.csv(manifest,file.path(out,"manifest.csv"),row.names=FALSE)
  utils::write.csv(qc,file.path(out,"validation_summary.csv"),row.names=FALSE)
  passed <- all(qc$passed)
  writeLines(c(paste("Release:",run_id),paste("QC:",if(passed)"PASS" else "FAIL"),
    "Contains pointers to existing RDAs; does not copy EHR data.",
    "Labs excluded; no maternal-to-delivery expansion.",
    "Clinical text-to-metadata crosswalk requires separate validation."),
    file.path(out,"README.txt"))
  message("Release files saved locally: ",out)
  message("Checks passed: ",sum(qc$passed),"/",nrow(qc))
  if (!passed) {
    print(qc[!qc$passed,,drop=FALSE])
    stop("Release QC FAILED; inspect validation_summary.csv. Inputs unchanged.")
  }
  message("Release QC PASSED; inputs unchanged.")
  # Only reports on successfully validated, checksum-pinned RDAs.
  # Missing pandoc/rmarkdown is nonfatal; CSV descriptive tables remain available.
  old_release_dir <- Sys.getenv("EHR_RELEASE_DIR", unset = NA_character_)
  on.exit({
    if (is.na(old_release_dir)) Sys.unsetenv("EHR_RELEASE_DIR")
    else Sys.setenv(EHR_RELEASE_DIR = old_release_dir)
  }, add = TRUE)
  Sys.setenv(EHR_RELEASE_DIR = normalizePath(out, winslash = "/", mustWork = TRUE))
  source("code/run/08_build_release_report.R", local = TRUE)
})
