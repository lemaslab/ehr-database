##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        January 10, 2018 
# IRB:
# Description: Link mom-baby data that has been extracted from RedCap. 
# Data: C:\Users\Dominick\Dropbox (UFL)\IRB\UF\UFHealth\redcap_import

# **************************************************************************** #
# ***************                Directory Variables           *************** #
# **************************************************************************** #

# Computer
# location="djlemas";location
# location="Dominick";location

# Directory Locations
work.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\redcap_import\\raw_data\\",sep="");work.dir
data.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\redcap_import\\raw_data\\",sep="");data.dir
out.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\redcap_import\\redcap_import_Jan18\\",sep="");out.dir

# Set Working Directory
setwd(work.dir)
list.files()

# **************************************************************************** #
# ***************                Library                       *************** #
# **************************************************************************** #

library(readxl)
library(data.table)
library(tidyr)
library(dplyr)
library(reshape2)

# **************************************************************************** #
# ***************                Baby.xlsx                                              
# **************************************************************************** #      

# file parameters
n_max=10000
data.file.name="Baby.xlsx";data.file.name


# **************************************************************************** #
# ***************                baby_demography                                              
# **************************************************************************** #

# baby_demography
#-----------------
# rows: 16684
# cols: 10 
# unique id: 16684
# repeat: 1
# ICD9/10: NA

# Note: import baby_id and birth_date

baby.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Baby", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.dat

# rename
newdata=rename(baby.dat, part_id = `Baby-Id`, baby_dob=DOB, baby_race=Race, 
               baby_ethnicity=Ethnicity, baby_birth_wt_gr=`Birth Weight (grams)`, 
               baby_admit_date=`Admit Date`, baby_admit_source=`Admit Source`,
               baby_nicu_los=`NICU LOS`,baby_gest_age=Gestational_Age, delivery_mode=Delivery_Mode);newdata

# subset to only dob
dat2=newdata %>%
  select(part_id,baby_dob);head(dat2)

# **************************************************************************** #
# ***************                mom_baby_link                                              
# **************************************************************************** #

# Note: import mom-baby link

baby.mom=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Baby-Mom Link", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.mom

# rename
newdata.link=rename(baby.mom, part_id = `Baby-Id`, mom_id=`Mom-Id`)
names(newdata.link); head(newdata.link)

# **************************************************************************** #
# ***************                merge baby and mom data                                              
# **************************************************************************** #

# merge 
mom.baby.link <- left_join(dat2, newdata.link, by = c('part_id'));dim(mom.baby.link) # 16684
mdata.d=subset(mom.baby.link, is.na(mom_id)==F);dim(mdata.d) # 16607

# sort
newdata2.mombaby <- mdata.d[order(mdata.d$mom_id),]
names(newdata2.mombaby); head(newdata2.mombaby)

# rename data
mom.baby.merge=newdata2.mombaby; head(mom.baby.merge)

# drop un-needed objects
rm(baby.dat, baby.mom, dat2, mdata.d, mom.baby.link, newdata, newdata.link, newdata2.mombaby)

# **************************************************************************** #
# ***************                Mom Prenatals.xlsx                                              
# **************************************************************************** #      

# file parameters
n_max=10000
data.file.name="Mom Prenatals.xlsx";data.file.name

# **************************************************************************** #
# ***************                linked_mom_demography                                              
# **************************************************************************** #

# Note: import maternal demography and merge with "mom.baby.merge"

# read data
mom.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Mom", range = NULL, col_names = TRUE,
                  col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                  guess_max = min(1000, n_max));mom.dat

# rename
newdata.mom=rename(mom.dat, mom_id=`Mom-Id`, mom_race_link=Race, mom_ethnicity_link=Ethnicity)
names(newdata.mom); head(newdata.mom)

# merge
mom.baby.demo <- left_join(mom.baby.merge, newdata.mom, by = c('mom_id'));dim(mom.baby.demo) # 16607

# redcap_repeat_instrument
newdata3=mom.baby.demo
newdata3$redcap_repeat_instrument="linked_mom_demography"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("mom_id","baby_dob"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "mom_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 5
table(dt3$redcap_repeat_instance)

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3)
unique(dt3$redcap_event_name)
names(dt3);head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)

# drop baby_dob
dt4$baby_dob <- NULL 
dt5=dt4

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# drop un-needed objects
rm(chunks, dt,dt5, dt3, dt4, mom.baby.demo, mom.dat, newdata.mom, newdata3)

# **************************************************************************** #
# ***************                linked_mom_prenatal_apt                                               
# **************************************************************************** #

# Note: import maternal prenatal_apt and merge with "mom.baby.merge"

# read data
prenat.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Prenatals by Appt", range = NULL, col_names = TRUE,
                     col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                     guess_max = min(1000, n_max));prenat.dat

# rename
newdata=rename(prenat.dat, mom_id = `Mom ID`, mom_prenat_apt_date_link= `Appt Time`, mom_prenat_enc_type_link= `Enc Type`,mom_prenat_ht_link=Height, mom_prenat_wt_oz_link=Weight)
names(newdata); head(newdata)

# merge
mom.baby.visits <- left_join(newdata, mom.baby.merge, by = c('mom_id'));dim(mom.baby.visits) # 16607
head(mom.baby.visits)

# rename: mom_id
mom.baby.visits.2=rename(mom.baby.visits, mom_id3 = mom_id)
names(mom.baby.visits.2); head(mom.baby.visits.2)

# days_to variable(s)
mom.baby.visits.2$days2_prenatal_apt_link=as.Date(mom.baby.visits.2$mom_prenat_apt_date_link,format="%Y-%m-%d")-as.Date(mom.baby.visits.2$baby_dob,format="%Y-%m-%d")
head(mom.baby.visits.2);names(mom.baby.visits.2)

# dplyr for data wrangle
#----------------------
dat.new=mom.baby.visits.2 %>%
  group_by(part_id) %>%
    mutate(day2_cut=ifelse(days2_prenatal_apt_link >= -365 & days2_prenatal_apt_link<=0, 1, 0));head(dat.new);names(dat.new)

# subset
mdata.d=subset(dat.new, day2_cut==1);dim(mdata.d) # 32124
head(mdata.d)

# redcap_repeat_instrument
newdata3=mdata.d
newdata3$redcap_repeat_instrument="linked_mom_prenatal_apt"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("mom_id3","mom_prenat_apt_date_link"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "mom_id3"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 82
table(dt3$redcap_repeat_instance)

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3)
unique(dt3$redcap_event_name)
names(dt3);head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)

# compute "mom_prenat_ht_inch"
dt4$tmp=dt4$mom_prenat_ht_link
dt4$tmp=gsub("'"," ",dt4$tmp)
dt4$tmp=gsub('"'," ",dt4$tmp)
dt4$tmp=trimws(dt4$tmp, "b") 
dt4$tmp=gsub('  '," ",dt4$tmp)
dt4$mom_prenat_ht_inch_link=sapply(strsplit(as.character(dt4$tmp)," "), function(x){12*as.numeric(x[1]) + as.numeric(x[2])})
dt4=dt4[,-c("tmp")]

# compute "mom_prenat_wt_lb"
dt4$mom_prenat_wt_lb_link=dt4$mom_prenat_wt_oz_link/16

# format "mom_prenat_ht"
dt4$mom_prenat_ht_link=paste0("&",dt4$mom_prenat_ht_link,"&")
dt4$mom_prenat_ht_link=gsub(" ","_",dt4$mom_prenat_ht_link) 
head(dt4)

# drop baby_dob & days2_cut
dt4$baby_dob <- NULL 
dt4$day2_cut <- NULL 
dt5=dt4

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# drop un-needed objects
rm(chunks, dat.new, dt, dt3, dt4, dt5, mdata.d, mom.baby, mom.baby.visits, newdata, newdata3, prenat.dat, mom.baby.visits.2)

# **************************************************************************** #
# ***************                Mom Medications.xlsx                                              
# **************************************************************************** # 

# file parameters
n_max=10000
data.file.name="Mom Medications.xlsx";data.file.name

# **************************************************************************** #
# ***************                linked_mom_antibiotics_ip                                              
# **************************************************************************** #

# mom_antibiotics_ip
#-----------------
# rows: 51398
# cols: 4
# unique id: 6740
# repeat: 574
# ICD9/10: NA

# read data
mom.abxip.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Mom Antibiotics IP Admin", range = NULL, col_names = TRUE,
                        col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                        guess_max = min(1000, n_max));mom.abxip.dat

# rename
newdata=rename(mom.abxip.dat, mom_id = `Mom ID`, mom_prenat_abxip_date_link=`Taken Datetime`,mom_prenat_abxip_action_link= `MAR Action`, mom_prenat_abxip_link=Antibiotics)
head(newdata)

# merge
mom.baby.abxip <- left_join(newdata, mom.baby.merge, by = c('mom_id'));dim(mom.baby.abxip) # 16607
head(mom.baby.abxip)

# rename: mom_id
mom.baby.abxip=rename(mom.baby.abxip, mom_id4 = mom_id)
names(mom.baby.abxip); head(mom.baby.abxip)

# days_to variable(s)
mom.baby.abxip$days2_prenatal_abxip_link=as.Date(mom.baby.abxip$mom_prenat_abxip_date_link,format="%Y-%m-%d")-as.Date(mom.baby.abxip$baby_dob,format="%Y-%m-%d")
head(mom.baby.abxip);names(mom.baby.abxip)

# dplyr for data wrangle
#----------------------
dat.new=mom.baby.abxip %>%
  group_by(part_id) %>%
  mutate(day2_cut=ifelse(days2_prenatal_abxip_link >= -365 & days2_prenatal_abxip_link<=0, 1, 0));head(dat.new);names(dat.new)

# subset
mdata.d=subset(dat.new, day2_cut==1);dim(mdata.d) # 13046
head(mdata.d)

# redcap_repeat_instrument
newdata3=mdata.d
newdata3$redcap_repeat_instrument="linked_mom_antibiotics_ip"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("mom_id4","mom_prenat_abxip_date_link"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "mom_id4"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 438
table(dt3$redcap_repeat_instance)

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3)
unique(dt3$redcap_event_name)
names(dt3);head(dt3)

# characters
dt3$mom_prenat_abxip_link=gsub(" ","_",dt3$mom_prenat_abxip_link) 
dt3$mom_prenat_abxip_link=gsub(",","&",dt3$mom_prenat_abxip_link) 
head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)

# drop baby_dob & days2_cut
dt4$baby_dob <- NULL 
dt4$day2_cut <- NULL 
dt5=dt4

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# drop un-needed objects
rm(chunks, dat.new, dt, dt3, dt4, dt5, mdata.d, mom.baby.abxip, newdata, newdata3, mom.abxip.dat)

# **************************************************************************** #
# ***************                linked_mom_prenatal_abx_rx                                              
# **************************************************************************** #

# mom_antibiotics_rx
#-----------------
# rows: 22927
# cols: 3
# unique id: 5787
# repeat: 55
# ICD9/10: NA

# read data
mom.abxscript.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Mom Antibiotics Prescription", range = NULL, col_names = TRUE,
                            col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                            guess_max = min(1000, n_max));mom.abxscript.dat

# rename
newdata=rename(mom.abxscript.dat, mom_id = `Mom ID`, mom_prenat_abxrx_date_link=`Order Datetime`, mom_prenat_abxrx_link=Antibiotics)
head(newdata)

# merge
mom.baby.abxrx <- left_join(newdata, mom.baby.merge, by = c('mom_id'));dim(mom.baby.abxrx) # 16607
head(mom.baby.abxrx)

# rename: mom_id
mom.baby.abxrx=rename(mom.baby.abxrx, mom_id5 = mom_id)
names(mom.baby.abxrx); head(mom.baby.abxrx)

# days_to variable(s)
mom.baby.abxrx$days2_prenatal_abxrx_link=as.Date(mom.baby.abxrx$mom_prenat_abxrx_date_link,format="%Y-%m-%d")-as.Date(mom.baby.abxrx$baby_dob,format="%Y-%m-%d")
head(mom.baby.abxrx);names(mom.baby.abxrx)

# dplyr for data wrangle
#----------------------
dat.new=mom.baby.abxrx %>%
  group_by(part_id) %>%
  mutate(day2_cut=ifelse(days2_prenatal_abxrx_link >= -365 & days2_prenatal_abxrx_link<=0, 1, 0));head(dat.new);names(dat.new)

# subset
mdata.d=subset(dat.new, day2_cut==1);dim(mdata.d) # 4879
head(mdata.d)

# redcap_repeat_instrument
newdata3=mdata.d
newdata3$redcap_repeat_instrument="linked_mom_prenatal_abx_rx"
names(newdata3);head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("mom_id5","mom_prenat_abxrx_date_link"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "mom_id5"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 22
table(dt3$redcap_repeat_instance)

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3)
unique(dt3$redcap_event_name)
names(dt3);head(dt3)

# characters
dt3$mom_prenat_abxrx_link=gsub(" ","_",dt3$mom_prenat_abxrx_link) 
head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)

# drop baby_dob & days2_cut
dt4$baby_dob <- NULL 
dt4$day2_cut <- NULL 
dt5=dt4

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# drop un-needed objects
rm(chunks, dat.new, dt, dt3, dt4, dt5, mdata.d, mom.baby.abxip, newdata, newdata3, mom.abxscript.dat, mom.baby.abxrx)

# **************************************************************************** #
# ***************                linked_mom_prental_meds_rx                                              
# **************************************************************************** #

# mom_medications_rx
#-----------------
# rows: 23357
# cols: 3
# unique id: 5811
# repeat: 55
# ICD9/10: NA

# read data
mom.script.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Mom Prescriptions", range = NULL, col_names = TRUE,
                         col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                         guess_max = min(1000, n_max));mom.script.dat

# rename
newdata=rename(mom.script.dat, mom_id = `Mom ID`, mom_med_rx_date_link=`Order Datetime`, mom_prenat_med_rx_link=Medication)
head(newdata)

# merge
mom.baby.script <- left_join(newdata, mom.baby.merge, by = c('mom_id'));dim(mom.baby.script) # 16607
head(mom.baby.script)

# rename: mom_id
mom.baby.script=rename(mom.baby.script, mom_id6 = mom_id)
names(mom.baby.script); head(mom.baby.script)

# days_to variable(s)
mom.baby.script$days2_prenat_med_rx_link=as.Date(mom.baby.script$mom_med_rx_date_link,format="%Y-%m-%d")-as.Date(mom.baby.script$baby_dob,format="%Y-%m-%d")
head(mom.baby.script);names(mom.baby.script)

# dplyr for data wrangle
#----------------------
dat.new=mom.baby.script %>%
  group_by(part_id) %>%
  mutate(day2_cut=ifelse(days2_prenat_med_rx_link >= -365 & days2_prenat_med_rx_link<=0, 1, 0));head(dat.new);names(dat.new)

# subset
mdata.d=subset(dat.new, day2_cut==1);dim(mdata.d) # 13046
head(mdata.d)

# redcap_repeat_instrument
newdata3=mdata.d
newdata3$redcap_repeat_instrument="linked_mom_prental_meds_rx"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("mom_id6","mom_med_rx_date_link"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "mom_id6"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 22
table(dt3$redcap_repeat_instance)

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3)
unique(dt3$redcap_event_name)
names(dt3);head(dt3)

# characters
dt3$mom_prenat_med_rx_link=gsub(" ","_",dt3$mom_prenat_med_rx_link) 
head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)

# drop baby_dob & days2_cut
dt4$baby_dob <- NULL 
dt4$day2_cut <- NULL 
dt5=dt4

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# drop un-needed objects
rm(chunks, dat.new, dt, dt3, dt4, dt5, mdata.d, mom.script.dat, newdata, newdata3, mom.baby.script, )

# **************************************************************************** #
# ***************                mom_medications_ip 1 & 2 (start here, name changes)                                            
# **************************************************************************** #

# mom_medications_ip 
#-----------------
# rows: 1107716
# cols: 4
# unique id: 9239
# repeat: 11205
# ICD9/10: NA

# read data1
mom.medip1.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Mom IP Medications", range = NULL, col_names = TRUE,
                         col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                         guess_max = min(1000, n_max));mom.medip1.dat

# read data2
mom.medip2.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Mom IP Medications(1)", range = NULL, col_names = TRUE,
                         col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                         guess_max = min(1000, n_max));mom.medip2.dat

# combine datasets
mom.meds.dat=bind_rows(mom.medip1.dat,mom.medip2.dat);mom.meds.dat

# rename
newdata=rename(mom.meds.dat, mom_id = `Mom ID`, mom_medip_date6=`Taken Datetime`, mom_prenat_medip6=Medication, mom_prenat_med_act6=`MAR Action`)
head(newdata)

# merge
mom.baby.meds <- left_join(newdata, mom.baby.merge, by = c('mom_id'));dim(mom.baby.meds) # 16607
head(mom.baby.meds)

# rename: mom_id
mom.baby.meds=rename(mom.baby.meds, mom_id6 = mom_id)
names(mom.baby.meds); head(mom.baby.meds)

# days_to variable(s)
mom.baby.meds$days2_prenatal_medip4=as.Date(mom.baby.meds$mom_medip_date6,format="%Y-%m-%d")-as.Date(mom.baby.meds$baby_dob,format="%Y-%m-%d")
head(mom.baby.meds);names(mom.baby.meds)

# dplyr for data wrangle
#----------------------
dat.new=mom.baby.meds %>%
  group_by(part_id) %>%
  mutate(day2_cut=ifelse(days2_prenatal_medip4 >= -365 & days2_prenatal_medip4<=0, 1, 0));head(dat.new);names(dat.new)

# subset
mdata.d=subset(dat.new, day2_cut==1);dim(mdata.d) # 4879
head(mdata.d)

# redcap_repeat_instrument
newdata3=mdata.d
newdata3$redcap_repeat_instrument="mom_baby_prenatal_med_ip"
names(newdata3);head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("mom_id6","mom_medip_date6"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "mom_id6"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 6984
table(dt3$redcap_repeat_instance)

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3)
unique(dt3$redcap_event_name)
names(dt3);head(dt3)

# characters
dt3$mom_prenat_medip6=gsub(" ","_",dt3$mom_prenat_medip6) 
head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)

# drop baby_dob & days2_cut
dt4$baby_dob <- NULL 
dt4$day2_cut <- NULL 
dt5=dt4

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop
