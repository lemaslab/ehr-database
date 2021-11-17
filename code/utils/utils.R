#' Get RedCap API Token
#' @return api token
get_API_token <- function(credential_label){
  # Get Emoncms API Token
  credential_path <- paste(Sys.getenv("USERPROFILE"), '\\DPAPI\\passwords\\', Sys.info()["nodename"], '\\', credential_label, '.txt', sep="")
  token<-decrypt_dpapi_pw(credential_path)
  return(token)
}

# import variables
n_max=1000000
data_file_name="part_id.csv"
data_dir=paste0("~/ehr-database/data/raw/part_id/")
data_import_path=paste0(data_dir,data_file_name)

# read data
part_id_list=read_csv(data_import_path, col_types = cols()) %>% 
  dplyr::rename(baby_id = Deidentified_baby_ID, 
                mom_id=Deidentified_mom_ID) %>%
  mutate(baby_id=paste0("baby-",baby_id),
         mom_id=paste0("mom-",mom_id)) 

# mom list
mom_list=unique(part_id_list$mom_id)

# baby list
baby_list=unique(part_id_list$baby_id)

# all list
part_id_list=c(mom_list,baby_list)

rm(data_file_name,n_max,data_dir,data_import_path)

event_list=c("2011_arm_1","2012_arm_1","2013_arm_1","2014_arm_1",
             "2015_arm_1","2016_arm_1","2017_arm_1","2018_arm_1",
             "2019_arm_1","2020_arm_1","2021_arm_1","na_arm_1")