##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #
           
# Author:      Dominick Lemas 
# Date:        January 10, 2018 
# IRB:
# Description: Import infant birth data to RedCap from flat files. 
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
out.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\redcap_import\\redcap_import_June18\\",sep="");out.dir

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
# ***************                Baby.xlsx                                              
# **************************************************************************** #      

# file parameters
n_max=1000000
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

# read data
baby.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Baby", range = NULL, col_names = TRUE,
          col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
          guess_max = min(1000, n_max));baby.dat

# data
dat=baby.dat

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_dob=DOB, baby_race=Race, 
               baby_ethnicity=Ethnicity, baby_birth_wt_gr=`Birth Weight (grams)`, 
               baby_admit_date=`Admit Date`, baby_admit_source=`Admit Source`,
               baby_nicu_los=`NICU LOS`,baby_gest_age=Gestational_Age, delivery_mode=Delivery_Mode);newdata

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 16684
length(newdata$part_id)         # 16684
names(newdata); head(newdata)

# sort (not needed in this case)
newdata2=newdata

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="participant_demographics"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt3=newdata3            
dt3$redcap_repeat_instance=1

# redcap_event_name
dt3$redcap_event_name="visit_1_arm_1"

# characters
dt3$delivery_mode=gsub(" ","_",dt3$delivery_mode) 
dt3$delivery_mode=gsub(",","&",dt3$delivery_mode) 
head(dt3);names(dt3)

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)
dt5=dt4

# export data
#-------------
batchSize=10000; # number of rows in single output file
data.file.name.export=as.character(dt5[2,2]);data.file.name.export

chunks=split(dt5, floor(0:(nrow(dt5)-1)/batchSize))
for (i in 1:length(chunks))
  { # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep="\t")
  } # end second loop

# clear slate
rm(baby.dat, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_vaccines                                               
# **************************************************************************** #

# baby_vaccines
#-----------------
# rows: 105611
# cols: 3
# unique id: 13068
# repeat: 34
# ICD9/10: NA

# read data
baby.vaccine=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Vaccines", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.vaccine

# data
dat=baby.vaccine

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_immune_date=Immune_Date, baby_vac_name=Immunization_Name)
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 13068
length(newdata$part_id)         # 105611
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_immune_date,format='%Y-%m-%d %H:%M:%S')),]
names(newdata2); head(newdata2)

# character format for immunization name
newdata2$baby_vac_name=gsub(" ","_",newdata2$baby_vac_name) 
newdata2$baby_vac_name=gsub(",","&",newdata2$baby_vac_name) 

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_vaccines"

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_immune_date","baby_vac_name","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 34

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

# clear slate
rm(baby.vaccine, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_wellvisit                                               
# **************************************************************************** #

# baby_wellvisit
#-----------------
# rows: 51441
# cols: 5
# unique id: 7667
# repeat: 43
# ICD9/10: NA

# read data
baby.well=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Well Visit", range = NULL, col_names = TRUE,
                       col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                       guess_max = min(1000, n_max));baby.well

# data
dat=baby.well

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_obs_date=Observation_Date, baby_ht_cm=`Height (cm)`, baby_wt_kgs=`Weight (kgs)`, baby_head_circ_cm=`Head Circumference (cm)`)
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 
length(newdata$part_id)         # 
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_obs_date,format='%Y-%m-%d %H:%M:%S')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_wellvisit"

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_obs_date","baby_ht_cm","baby_wt_kgs","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 43

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

# clear slate
rm(baby.well, dat, newdata, newdata2,chunks, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                mom_baby_link   (need to modify data dictionary and re-import)                                            
# **************************************************************************** #

# baby_mom_baby_link
#-----------------
# rows: 29798
# cols: 2
# unique id: 28069
# repeat: 5
# ICD9/10: NA

# read data
baby.mom=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Baby-Mom Link", range = NULL, col_names = TRUE,
                    col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                    guess_max = min(1000, n_max));baby.mom

# data
dat=baby.mom

# rename
newdata=rename(dat, part_id = `Baby-Id`, mom_id=`Mom-Id`)
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 28069
length(newdata$part_id)         # 29798
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="mom_baby_link"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "part_link","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
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

# clear slate
rm(baby.mom, dat, newdata, chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_first_height                                               
# **************************************************************************** #

# baby_first_height
#-----------------
# rows: 16441
# cols: 3
# unique id: 16441
# repeat: 1
# ICD9/10: NA

# read data
baby.ht=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "First Height", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.ht

# data
dat=baby.ht

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_ht1_date=`1st Height date`, baby_ht1_cm=`Height (cm)`);newdata
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 16441
length(newdata$part_id)         # 16441
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_ht1_date,format='%Y-%m-%d %H:%M:%S')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_first_height"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_ht1_date","baby_ht1_cm","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 1

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

# clear slate
rm(baby.ht, dat, newdata, chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_first_head_circumference                                               
# **************************************************************************** #

# baby_first_head_circumference
#-----------------
# rows: 16353
# cols: 3
# unique id: 16346
# repeat: 2
# ICD9/10: NA

# read data
baby.hc=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "First Head Circumference", range = NULL, col_names = TRUE,
                  col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                  guess_max = min(1000, n_max));baby.hc

# data
dat=baby.hc

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_hc1_date=`1st Head Circumference date`, baby_hc1_cm=`Head Circumference (cm)`);newdata
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 16346
length(newdata$part_id)         # 16353
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_hc1_date,format='%Y-%m-%d %H:%M:%S')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_first_head_circumference"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_hc1_date","baby_hc1_cm","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 2

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

# clear slate
rm(baby.hc, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_antibiotics_rx                                               
# **************************************************************************** #

# baby_antibiotics_rx
#-----------------
# rows: 5156
# cols: 5
# unique id: 2717
# repeat: 25
# ICD9/10: NA

# read data
baby.script=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Antibiotics Prescription", range = NULL, col_names = TRUE,
                  col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                  guess_max = min(1000, n_max));baby.script

# data
dat=baby.script

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_med_order=`Med Order #`, baby_med_code=Med_Code, baby_meds=Medication, baby_med_date=`Med Order Datetime`);newdata
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 2717
length(newdata$part_id)         # 5156
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_med_date,format='%Y-%m-%d %H:%M:%S')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_antibiotics_rx"
names(newdata3); head(newdata3)

# modify medication string
newdata3$baby_meds=gsub(" ","_",newdata3$baby_meds) 
newdata3$baby_meds=gsub(",","&",newdata3$baby_meds) 
head(newdata3);str(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_med_date","baby_med_order","baby_med_code","baby_meds","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 25

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

# clear slate
rm(baby.script, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_antibiotics_ip                                               
# **************************************************************************** #

# baby_antibiotics_ip
#-----------------
# rows: 70367
# cols: 6
# unique id: 15998
# repeat: 537
# ICD9/10: NA

# read data
baby.abx=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Antibiotics IP Administration", range = NULL, col_names = TRUE,
                      col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = min(1000, n_max));baby.abx

# data
dat=baby.abx

# rename
newdata=rename(dat, part_id = `Baby-Id`,baby_med_order_ip="Med Order #",baby_mar_action_ip="MAR_Action",baby_med_code_ip="Med_Code",baby_med_ip_date="Taken_Datetime",baby_med_ip="Medication");newdata
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 15998
length(newdata$part_id)         # 70367
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_med_ip_date,format='%Y-%m-%d %H:%M:%S')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_antibiotics_ip"
names(newdata3); head(newdata3)

# modify medication string
newdata3$baby_med_ip=gsub(" ","_",newdata3$baby_med_ip) 
newdata3$baby_med_ip=gsub(",","&",newdata3$baby_med_ip) 
head(newdata3);str(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3);names(dt)           
setkeyv(dt, c("part_id", "baby_med_ip_date","baby_med_order_ip","baby_mar_action_ip","baby_med_code_ip","baby_med_ip","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) #537

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

# clear slate
rm(baby.abx, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                mom_baby                                               
# **************************************************************************** #

# mom_baby
#-----------------
# rows: 16649
# cols: 5
# unique id: 15998
# repeat: 537
# ICD9/10: NA

# read data
mom.baby=read_xlsx(paste0(data.dir,data.file.name), sheet = "Mom-Baby", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));mom.baby

# data
dat=mom.baby

# rename
newdata=rename(dat, part_id = `Baby-Id`,baby_gender=`Baby Gender`,mom_id2=`Mom-Id`,mom_age_delivery=`Mom Age`,payer="Payer");newdata
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 16649
length(newdata$part_id)         # 16649
names(newdata); head(newdata)

# sort
# dont need to sort. no dates

# redcap_repeat_instrument
newdata3=newdata
newdata3$redcap_repeat_instrument="baby_demography"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt3=newdata3            
dt3$redcap_repeat_instance=1

# redcap_event_name
dt3$redcap_event_name="visit_1_arm_1"

# order columns for export
col.names=names(dt3);col.names
col.first=c("part_id","redcap_repeat_instrument","redcap_repeat_instance","redcap_event_name");col.first
col.next=subset(col.names, !(col.names%in%col.first));col.next
colFixed=append(col.first, col.next, after=length(col.first));colFixed
dt4=setcolorder(dt3, colFixed)
names(dt4);head(dt4)
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

# clear slate
rm(mom.baby, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

