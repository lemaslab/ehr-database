# Postprocess existing standalone lab RDAs. Never alter the source RDAs.
# Set EHR_LAB_SOURCE_TAG to the original extraction timestamp (default 20261010_193021).
# Set EHR_LAB_LINKAGE_DATE to the validated linkage date (default 20261010).
local({
  source("code/functions/utils_write_dataset.R")
  if(!requireNamespace("dplyr",quietly=TRUE)) stop("Requires dplyr")
  root <- file.path(getwd(),"data","processed")
  started <- Sys.time()
  log_step <- function(...) message(sprintf("[LAB %s] ",format(Sys.time(),"%H:%M:%S")), paste0(...,collapse=""))
  fmt <- function(x) format(x,big.mark=",",scientific=FALSE,trim=TRUE)
  log_step("START canonical laboratory linkage")
  log_step("Repository: ", normalizePath(getwd(),winslash="/",mustWork=TRUE))
  log_step("Processed root: ", normalizePath(root,winslash="/",mustWork=FALSE))
  log_step("Mode: ", Sys.getenv("EHR_OUTPUT_MODE","release"),
           " | network sync: ",Sys.getenv("EHR_NETWORK_SYNC","true"),
           " | local CSV: ",Sys.getenv("EHR_LOCAL_CSV","false"))
  stamp <- Sys.getenv("EHR_LAB_SOURCE_TAG","20261010_193021")
  pilot_n <- Sys.getenv("EHR_LAB_PILOT_N","")
  pilot <- nzchar(pilot_n)
  if(pilot) {
    pilot_n <- suppressWarnings(as.integer(pilot_n))
    if(is.na(pilot_n) || pilot_n < 1L) stop("EHR_LAB_PILOT_N must be positive")
    message("[LAB PILOT] Local only; maximum rows per source: ",pilot_n)
  }
  release_date <- Sys.getenv("EHR_RELEASE_DATE",format(Sys.Date(),"%Y%m%d"))
  if(!grepl("^[0-9]{8}$",release_date)) stop("EHR_RELEASE_DATE must be YYYYMMDD")
  reference_date <- Sys.getenv("EHR_LAB_LINKAGE_DATE",release_date)
  if(!pilot && nzchar(Sys.getenv("EHR_RELEASE_DATE","")) && reference_date!=release_date)
    stop("Lab linkage must use same-date canonical linkage for full release: ",release_date)
  reference_file <- file.path(root,"COMBINED",paste0("mom_baby_link_all_sites_",reference_date,".rda"))
  log_step("Source tag: ",stamp," | linkage date: ",reference_date)
  log_step("Loading canonical linkage: ",reference_file)
  if(!file.exists(reference_file)) stop("Linkage missing: ",reference_file)
  env <- new.env(parent=emptyenv())
  load(reference_file,envir=env)
  reference <- env$mom_baby_link_all_sites
  stopifnot(is.data.frame(reference), !anyDuplicated(reference$part_id_infant))
  required_link <- c("site","part_id_mom","part_id_infant","delivery_id")
  missing_link <- setdiff(required_link,names(reference))
  if(length(missing_link)) stop("Canonical linkage missing columns: ",paste(missing_link,collapse=", "))
  log_step("Loaded linkage: ",fmt(nrow(reference))," rows; ",
           fmt(length(unique(reference$part_id_mom)))," mothers; ",
           fmt(length(unique(reference$part_id_infant)))," infants")
  log_step("Linkage variables: ",paste(names(reference),collapse=", "))
  maternal_ids <- unique(as.character(reference$part_id_mom))
  infant_ids <- as.character(reference$part_id_infant)
  infant_deliveries <- as.character(reference$delivery_id)
  rm(reference,env)
  gc(verbose=FALSE)
  results <- list()
  sources <- list()
  for(role in c("mom","infant")) for(site in c("gnv","jax")) {
    key <- paste(role,site,sep="_")
    source_start <- Sys.time()
    log_step("------------------------------------------------------------")
    log_step("BEGIN ",key," (",toupper(site),", ",role,")")
    subdir <- toupper(site)
    dataset <- paste0(role,"_labs_",site)
    source_file <- file.path(root,subdir,paste0(dataset,"_",stamp,".rda"))
    log_step("Input RDA: ",source_file)
    if(!file.exists(source_file)) stop("Missing extracted lab RDA: ",source_file)
    log_step("Input size: ",round(file.info(source_file)$size/1024^3,3)," GiB; loading...")
    en <- new.env(parent=emptyenv())
    objects <- load(source_file,envir=en)
    log_step("Objects found in RDA: ",paste(objects,collapse=", "))
    if(!dataset %in% objects) stop("Unexpected lab object in ",source_file)
    x <- get(dataset,envir=en)
    if(!is.data.frame(x)) stop("Lab object is not a data.frame: ",dataset)
    log_step("Loaded ",fmt(nrow(x))," records, ",ncol(x)," columns")
    log_step("Variables: ",paste(names(x),collapse=", "))
    required_lab <- c("lab_participant_id","specimen_datetime","lab_result_raw","loinc_code")
    missing_lab <- setdiff(required_lab,names(x))
    if(length(missing_lab)) stop(key," missing required lab variables: ",
       paste(missing_lab,collapse=", "))
    log_step("Required variable check: PASS (",paste(required_lab,collapse=", "),")")
    if(pilot) {
      x <- utils::head(x,pilot_n)
      log_step("Pilot subset: ",fmt(nrow(x))," rows")
    }
    n <- nrow(x)
    candidates <- as.character(x$lab_participant_id)
    matched <- !is.na(candidates) & candidates %in%
      if(role=="mom") maternal_ids else infant_ids
    log_step("Canonical ID check: matched ",fmt(sum(matched)),"/",fmt(n),
             " (",round(100*mean(matched),4),"%); unmatched ",fmt(sum(!matched)))
    log_step("Specimen timestamps missing: ",fmt(sum(is.na(x$specimen_datetime))),
             " | raw lab results missing: ",fmt(sum(is.na(x$lab_result_raw))),
             " | LOINC missing: ",fmt(sum(is.na(x$loinc_code))))
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
    log_step("Canonical IDs populated; ",
             if(role=="mom") "maternal delivery ID intentionally remains NA"
             else paste0("infant delivery IDs assigned: ",fmt(sum(!is.na(x$delivery_id)))) )
    output_name <- dataset
    destination <- if(pilot) file.path("LAB_PILOT",subdir) else subdir
    output_file <- file.path(root,destination,paste0(output_name,"_",release_date,".rda"))
    log_step("Output directory: ",file.path(root,destination))
    log_step("Output RDA: ",output_file)
    if(!file.exists(output_file) || tolower(Sys.getenv("EHR_FORCE","false")) %in% c("true","1","yes")) {
      log_step("Writing dataset to local storage",if(!pilot && tolower(Sys.getenv("EHR_NETWORK_SYNC","true")) %in% c("true","1","yes")) " and network" else " (network disabled)","...")
      write_dataset(x,output_name,working_dir=getwd(),subdir=destination,
        write_network=!pilot)
      log_step("Write finished: ",output_file," (",round(file.info(output_file)$size/1024^3,3)," GiB)")
    } else {
      log_step("SKIP existing RDA (not overwritten): ",output_file)
    }
    log_step("Completed ",key," in ",round(as.numeric(difftime(Sys.time(),source_start,units="secs")),1)," seconds")
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
      log_step("Combining ",role," labs from GNV + JAX")
      parts <- lapply(c("gnv","jax"),function(site) {
        input <- sources[[paste(role,site,sep="_")]]
        e <- new.env(parent=emptyenv())
        log_step("Loading combined input: ",input)
        obj <- load(input,envir=e)
        get(obj[[1]],envir=e,inherits=FALSE)
      })
      combined <- dplyr::bind_rows(parts)
      stopifnot(nrow(combined)==sum(vapply(parts,nrow,integer(1))))
      log_step("Combined ",role," rows: ",fmt(nrow(combined)),
               " | site rows: ",paste(vapply(parts,nrow,integer(1)),collapse=" + "))
      combined_name <- paste0(role,"_labs_all_sites")
      destination <- if(pilot) file.path("LAB_PILOT","COMBINED") else "COMBINED"
      target <- file.path(root,destination,
        paste0(combined_name,"_",release_date,".rda"))
      log_step("Combined output: ",target)
      if(!file.exists(target) || tolower(Sys.getenv("EHR_FORCE","false")) %in% c("true","1","yes")) {
        log_step("Writing combined ",role," RDA...")
        write_dataset(combined,combined_name,working_dir=getwd(),
          subdir=destination,write_network=!pilot)
        log_step("Combined write finished: ",target," (",round(file.info(target)$size/1024^3,3)," GiB)")
      } else log_step("SKIP existing combined RDA: ",target)
      rm(parts,combined);gc(verbose=FALSE)
    }
  }
  audit <- do.call(rbind,results)
  qc_dir <- file.path(root,if(pilot)"LAB_PILOT_QC" else "LAB_QC")
  dir.create(qc_dir,recursive=TRUE,showWarnings=FALSE)
  audit_file <- file.path(qc_dir,paste0("lab_qc_",release_date,".csv"))
  utils::write.csv(audit,audit_file,row.names=FALSE)
  print(audit[,c("dataset","records","matched_records","unmatched_records","match_pct")],row.names=FALSE)
  log_step("QC summary:")
  message(paste(capture.output(print(audit[,c("dataset","records","matched_records","unmatched_records","match_pct")],row.names=FALSE)),collapse="\n"))
  log_step("QC file saved: ",audit_file)
  log_step("FINISHED in ",round(as.numeric(difftime(Sys.time(),started,units="mins")),2)," minutes")
  if(pilot) message("[LAB PILOT] All outputs isolated in data/processed/LAB_PILOT; network skipped.")
  if(any(audit$unmatched_records)) warning("Unmatched labs retained; inspect QC.")
})
