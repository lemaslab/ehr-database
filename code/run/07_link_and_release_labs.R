# Postprocess existing standalone lab RDAs. Never alter the source RDAs.
# Set EHR_LAB_SOURCE_TAG to the original extraction timestamp (default 20261010_193021).
# Set EHR_LAB_LINKAGE_DATE to the validated linkage date (default 20261010).
local({
  source("code/functions/utils_write_dataset.R")
  if(!requireNamespace("dplyr",quietly=TRUE)) stop("Requires dplyr")
  root <- file.path(getwd(),"data","processed")
  stamp <- Sys.getenv("EHR_LAB_SOURCE_TAG","20261010_193021")
  pilot_n <- Sys.getenv("EHR_LAB_PILOT_N","")
  pilot <- nzchar(pilot_n)
  if(pilot) {
    pilot_n <- suppressWarnings(as.integer(pilot_n))
    if(is.na(pilot_n) || pilot_n < 1L) stop("EHR_LAB_PILOT_N must be positive")
    message("[LAB PILOT] Local only; maximum rows per source: ",pilot_n)
  }
  reference_date <- Sys.getenv("EHR_LAB_LINKAGE_DATE","20261010")
  reference_file <- file.path(root,"COMBINED",paste0("mom_baby_link_all_sites_",reference_date,".rda"))
  if(!file.exists(reference_file)) stop("Linkage missing: ",reference_file)
  env <- new.env(parent=emptyenv())
  load(reference_file,envir=env)
  reference <- env$mom_baby_link_all_sites
  stopifnot(is.data.frame(reference), !anyDuplicated(reference$part_id_infant))
  maternal_ids <- unique(as.character(reference$part_id_mom))
  infant_ids <- as.character(reference$part_id_infant)
  infant_deliveries <- as.character(reference$delivery_id)
  rm(reference,env)
  gc(verbose=FALSE)
  results <- list()
  sources <- list()
  for(role in c("mom","infant")) for(site in c("gnv","jax")) {
    key <- paste(role,site,sep="_")
    subdir <- toupper(site)
    dataset <- paste0(role,"_labs_",site)
    source_file <- file.path(root,subdir,paste0(dataset,"_",stamp,".rda"))
    if(!file.exists(source_file)) stop("Missing extracted lab RDA: ",source_file)
    en <- new.env(parent=emptyenv())
    objects <- load(source_file,envir=en)
    if(!dataset %in% objects) stop("Unexpected lab object in ",source_file)
    x <- get(dataset,envir=en)
    if(pilot) x <- utils::head(x,pilot_n)
    n <- nrow(x)
    candidates <- as.character(x$lab_participant_id)
    matched <- !is.na(candidates) & candidates %in%
      if(role=="mom") maternal_ids else infant_ids
    x$lab_link_status <- ifelse(matched,"canonical_match",
      ifelse(is.na(candidates),"missing_source_id","unmatched_canonical_id"))
    if(role=="mom") {
      x$part_id_mom <- ifelse(matched,candidates,NA_character_)
      x$delivery_id <- NA_character_
    } else {
      x$part_id_infant <- ifelse(matched,candidates,NA_character_)
      x$delivery_id <- infant_deliveries[match(x$part_id_infant,infant_ids)]
      if(any(matched & is.na(x$delivery_id))) stop("Infant delivery relation missing: ",key)
    }
    stopifnot(nrow(x)==n)
    output_name <- dataset
    destination <- if(pilot) file.path("LAB_PILOT",subdir) else subdir
    output_file <- file.path(root,destination,paste0(output_name,"_",format(Sys.Date(),"%Y%m%d"),".rda"))
    if(!file.exists(output_file)) {
      write_dataset(x,output_name,working_dir=getwd(),subdir=destination,
        write_network=!pilot)
    } else message("[LAB] Reusing existing canonical RDA: ",output_file)
    results[[key]] <- data.frame(dataset=key,records=n,
      unique_source_patients=length(unique(stats::na.omit(candidates))),
      matched_records=sum(matched),unmatched_records=sum(!matched),
      match_pct=if(n)round(100*mean(matched),4) else NA_real_,
      missing_specimen_dates=sum(is.na(x$specimen_datetime)),
      missing_result=sum(is.na(x$lab_result_raw)),
      missing_loinc=sum(is.na(x$loinc_code)),
      source_file=source_file,output_file=output_file)
    sources[[key]] <- output_file
    rm(x,en,candidates,matched);gc(verbose=FALSE)
  }
  # Combine verified site outputs one role at a time; preserve originals.
  if(tolower(Sys.getenv("EHR_LAB_COMBINE","true")) %in% c("true","1","yes")) {
    for(role in c("mom","infant")) {
      parts <- lapply(c("gnv","jax"),function(site) {
        input <- sources[[paste(role,site,sep="_")]]
        e <- new.env(parent=emptyenv())
        obj <- load(input,envir=e)
        get(obj[[1]],envir=e,inherits=FALSE)
      })
      combined <- dplyr::bind_rows(parts)
      stopifnot(nrow(combined)==sum(vapply(parts,nrow,integer(1))))
      combined_name <- paste0(role,"_labs_all_sites")
      destination <- if(pilot) file.path("LAB_PILOT","COMBINED") else "COMBINED"
      target <- file.path(root,destination,
        paste0(combined_name,"_",format(Sys.Date(),"%Y%m%d"),".rda"))
      if(!file.exists(target)) {
        write_dataset(combined,combined_name,working_dir=getwd(),
          subdir=destination,write_network=!pilot)
      } else message("[LAB] Reusing existing combined linked RDA: ",target)
      rm(parts,combined);gc(verbose=FALSE)
    }
  }
  audit <- do.call(rbind,results)
  qc_dir <- file.path(root,if(pilot)"LAB_PILOT_QC" else "LAB_QC")
  dir.create(qc_dir,recursive=TRUE,showWarnings=FALSE)
  audit_file <- file.path(qc_dir,paste0("lab_qc_",format(Sys.Date(),"%Y%m%d"),".csv"))
  utils::write.csv(audit,audit_file,row.names=FALSE)
  print(audit[,c("dataset","records","matched_records","unmatched_records","match_pct")],row.names=FALSE)
  message("[LAB] QC: ",audit_file)
  if(pilot) message("[LAB PILOT] All outputs isolated in data/processed/LAB_PILOT; network skipped.")
  if(any(audit$unmatched_records)) warning("Unmatched labs retained; inspect QC.")
})
