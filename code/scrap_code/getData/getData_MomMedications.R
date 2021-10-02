##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #
           
# Author:      Dominick Lemas 
# Date:        January 06, 2018 
# IRB:
# Description: Import maternal medication data to to redcap from flat files. 
# Data: C:\Users\Dominick\Dropbox (UFL)\IRB\UF\UFHealth\redcap_import

# **************************************************************************** #
# ***************                Directory Variables           *************** #
# **************************************************************************** #

# Computer
# location="djlemas";location
# location="Dominick";location

# Directory Locations
work.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\IRB\\UF\\UFHealth\\redcap_import\\raw_data\\",sep="");work.dir
data.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\IRB\\UF\\UFHealth\\redcap_import\\raw_data\\",sep="");data.dir
out.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\IRB\\UF\\UFHealth\\redcap_import\\03_redcap_import_Jan18\\",sep="");out.dir

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

# **************************************************************************** #
# ***************                Mom Medications.xlsx                                              
# **************************************************************************** # 

# file parameters
n_max=10000
data.file.name="Mom Medications.xlsx";data.file.name

# **************************************************************************** #
# ***************                mom_antibiotics_ip                                              
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

# data
dat=mom.abxip.dat

# rename
newdata=rename(dat, part_id = `Mom ID`, mom_prenat_abxip_date=`Taken Datetime`,mom_prenat_abxip_action= `MAR Action`, mom_prenat_abxip=Antibiotics)
head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 6740
length(newdata$part_id)         # 51398
names(newdata); head(newdata)

# sort 
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$mom_prenat_abxip_date,format='%Y-%m-%d %H:%M:%S')),]
head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="mom_antibiotics_ip"
names(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id","mom_prenat_abxip_date","mom_prenat_abxip_action","mom_prenat_abxip","redcap_repeat_instrument"))  # Create key for data.table
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        # Create new variable
max(dt3$redcap_repeat_instance) # 574

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3); unique(dt3$redcap_event_name)
names(dt3)

# characters
dt3$mom_prenat_abxip=gsub(" ","_",dt3$mom_prenat_abxip) 
dt3$mom_prenat_abxip=gsub(",","&",dt3$mom_prenat_abxip) 
head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)
dt5=dt4

# what are unique medications?
unique(dt5$mom_prenat_abxip)

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# clear slate
rm(mom.abxip.dat, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                mom_antibiotics_rx                                              
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

# data
dat=mom.abxscript.dat

# rename
newdata=rename(dat, part_id = `Mom ID`, mom_prenat_abxrx_date=`Order Datetime`, mom_prenat_abxrx=Antibiotics)
head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 5787
length(newdata$part_id)         # 22937
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$mom_prenat_abxrx_date,format='%Y-%m-%d %H:%M:%S')),]
head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="mom_antibiotics_rx"
names(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id","mom_prenat_abxrx_date","mom_prenat_abxrx"))  # Create key for data.table
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        # Create new variable
max(dt3$redcap_repeat_instance) # 55

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3); unique(dt3$redcap_event_name)
names(dt3)

# characters
dt3$mom_prenat_abxrx=gsub(" ","_",dt3$mom_prenat_abxrx) 
head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)
dt5=dt4

# what are unique medications?
unique(dt5$mom_prenat_abxrx)

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# clear slate
rm(mom.abxscript.dat, dat, newdata, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                mom_medications_ip 1 & 2                                             
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

# data
dat=mom.meds.dat

# rename
newdata=rename(dat, part_id = `Mom ID`, mom_medip_date=`Taken Datetime`, mom_prenat_medip=Medication, mom_prenat_med_act=`MAR Action`)
head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 9239
length(newdata$part_id)         # 1107716
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$mom_medip_date,format='%Y-%m-%d %H:%M:%S')),]
head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="mom_medications_ip"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id","mom_medip_date","mom_prenat_med_act","mom_prenat_medip","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        # Create new variable
max(dt3$redcap_repeat_instance) # 11205

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3); unique(dt3$redcap_event_name)
names(dt3)

# characters
dt3$mom_prenat_medip=gsub(" ","_",dt3$mom_prenat_medip) 
head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)
dt5=dt4

# what are unique medications?
temp=unique(dt5$mom_prenat_medip);temp
# write.csv(temp, file="prenatal_medications_ip.csv")

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# clear slate
rm(mom.medip1.dat,mom.medip2.dat,mom.meds.dat, dat, newdata, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                mom_medications_rx                                              
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

# data
dat=mom.script.dat

# rename
newdata=rename(dat, part_id = `Mom ID`, mom_med_rx_date=`Order Datetime`, mom_prenat_med_rx=Medication)
head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 5811
length(newdata$part_id)         # 23357
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$mom_med_rx_date,format='%Y-%m-%d %H:%M:%S')),]
head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="mom_medications_rx"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id","mom_med_rx_date","mom_prenat_med_rx","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        # Create new variable
max(dt3$redcap_repeat_instance) # 55

# create "redcap_event_name" variable
dt3$redcap_event_name=paste("visit_",dt3$redcap_repeat_instance,"_arm_1",sep="")
head(dt3); unique(dt3$redcap_event_name)
names(dt3)

# characters
dt3$mom_prenat_med_rx=gsub(" ","_",dt3$mom_prenat_med_rx) 
head(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)
dt5=dt4

# what are unique medications?
temp=unique(dt5$mom_prenat_med_rx);temp
# write.csv(temp, file="prenatal_medications_rx.csv")

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
{ # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep=";")
} # end second loop

# clear slate
rm(mom.script.dat, dat, newdata, newdata2, newdata3, dt, dt3, dt4, dt5)