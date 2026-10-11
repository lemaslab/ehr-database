# Inventory of literal raw source paths in pipeline R scripts.
# Usage: source("code/run/00_raw_source_inventory.R")
# Produces reviewable aggregate metadata only; never copies raw EHR data.
ehr_raw_source_inventory <- function(root=getwd(),
                                     output_dir=file.path(root,"logs","raw_source_inventory"),
                                     hash_files=FALSE) {
  scripts <- list.files(file.path(root,"code"),pattern="\\.[Rr]$",
                        recursive=TRUE,full.names=TRUE)
  if (!length(scripts)) stop("No R source files found under code/")
  records <- list()
  for (script in scripts) {
    lines <- readLines(script,warn=FALSE)
    for (i in seq_along(lines)) {
      # Detect V:/ paths embedded in quotes; retain original source spelling.
      matches <- regmatches(lines[i],gregexpr("[Vv]:/[^\"']+\\.(csv|tsv|txt|parquet|rda|rds|xlsx|xls|zip|gz)",lines[i],
                                          perl=TRUE,ignore.case=TRUE))[[1]]
      if (!length(matches)) next
      records[[length(records)+1L]] <- data.frame(
        processing_script=gsub("\\\\","/",substring(script,nchar(root)+2L)),
        script_line=i, source_path=matches, stringsAsFactors=FALSE)
    }
  }
  raw <- if (length(records)) do.call(rbind,records) else data.frame(
    processing_script=character(),script_line=integer(),source_path=character())
  raw <- unique(raw)
  raw$source_filename <- basename(raw$source_path)
  raw$site <- ifelse(grepl("jax|jacksonville",raw$source_path,ignore.case=TRUE),"JAX",
              ifelse(grepl("gnv|gainesville",raw$source_path,ignore.case=TRUE),"GNV","REVIEW"))
  raw$file_exists <- file.exists(raw$source_path)
  fi <- file.info(raw$source_path)
  raw$size_bytes <- as.numeric(fi$size)
  raw$modified_at <- as.character(fi$mtime)
  raw$md5 <- NA_character_
  if (hash_files && any(raw$file_exists)) {
    message("Hashing raw files may be slow for large EHR source datasets.")
    raw$md5[raw$file_exists] <- unname(tools::md5sum(raw$source_path[raw$file_exists]))
  }
  raw$manual_review <- ifelse(raw$file_exists,"CHECK_SOURCE_IDENTITY","MISSING_OR_UNAVAILABLE")
  dir.create(output_dir,recursive=TRUE,showWarnings=FALSE)
  stamp <- format(Sys.time(),"%Y%m%d_%H%M%S")
  out <- file.path(output_dir,paste0("raw_source_inventory_",stamp,".csv"))
  utils::write.csv(raw,out,row.names=FALSE,na="")
  # Indicate nonliteral/dynamically assembled sources that cannot be automatically
  # resolved from string extraction; the inventory is not proof of completeness.
  dynamic <- data.frame(processing_script=character(),line=integer(),code=character())
  for (script in scripts) {
    lines <- readLines(script,warn=FALSE)
    hit <- which(grepl("read_csv\\(|read_tsv\\(|read_delim\\(|read_parquet\\(|readRDS\\(|read_excel\\(|fread\\(|arrow::open_dataset\\(",lines,perl=TRUE))
    if (length(hit)) dynamic <- rbind(dynamic,data.frame(
      processing_script=rep(gsub("\\\\","/",substring(script,nchar(root)+2L)),length(hit)),
      line=hit,code=trimws(lines[hit])))
  }
  dynamic_path <- file.path(output_dir,paste0("read_calls_review_",stamp,".csv"))
  utils::write.csv(dynamic,dynamic_path,row.names=FALSE)
  message("[RAW INPUTS] Inventory: ",out)
  message("[RAW INPUTS] Read-call review: ",dynamic_path)
  message("[RAW INPUTS] Paths found: ",nrow(raw),"; unavailable: ",sum(!raw$file_exists))
  message("[RAW INPUTS] Review site=REVIEW and read_calls_review to confirm dynamic inputs.")
  invisible(list(inventory=out,read_calls=dynamic_path,data=raw))
}
if (sys.nframe()==0L) ehr_raw_source_inventory()
