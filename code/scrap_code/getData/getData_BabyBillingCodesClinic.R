##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #
           
# Author:      Dominick Lemas 
# Date:        January 05, 2018 
# IRB:
# Description: Import infant clinic data to RedCap from flat files. 
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
# ***************                Baby-Billing Codes (Clinic).xlsx                                              
# **************************************************************************** #      

# file parameters
n_max=10000
data.file.name="Baby-Billing Codes (Clinic).xlsx";data.file.name

# **************************************************************************** #
# ***************                baby_clinic_asthma                                               
# **************************************************************************** #

# baby_clinic_asthma
#-----------------
# rows: 13241
# cols: 3
# unique id: 3577
# repeat: 75
# ICD9/10: "465.9"   "493.90"  "493.02"  "J45.20"  "493.92"  "493.00"  "J45.998" "J45.909" "J45.41"  "J45.901" "J06.9"   "466.0"   "493.01"  "J45.30"  "J45.21"  "493.82"  "493.91"  "J45.31" 
# "J45.902" "J45.40"  "J45.50"  "J45.22"  "J45.32"  "J45.51"  "J45.42"  "J20.9"   "J45.52"  "493.20"  "465.8"   "493.11"  "493.10"  "J45.991" "493.81"  "J44.9"   "491.21"  "J44.1"

# read data
baby.asthma=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Asthma (Clinic)", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.asthma

# data
dat=baby.asthma

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_asthma_date=`Service/Charge Date`, baby_asthma_icd=`Asthma ICD9/ICD10`);newdata
unique(newdata$baby_asthma_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 3577
length(newdata$part_id)         # 13241
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_asthma_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_asthma"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_asthma_date","baby_asthma_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 75

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
rm(baby.asthma, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_clinic_foodallergy                                               
# **************************************************************************** #

# baby_clinic_foodallergy
#-----------------
# rows: 2360
# cols: 3
# unique id: 1372 
# repeat: 33
# ICD9/10: "787.91"   "909.9"    "Z91.018"  "708.0"    "Z91.010"  "995.7"    "V15.05"   "558.3"    "995.60"   "V58.89"   "V15.01"   "T78.1XXA" "909.0"    "L50.0"    "K52.29"   "T78.00XA"
#          "T78.1XXD"

# read data
baby.allergy=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Food Allergy (Clinic)", range = NULL, col_names = TRUE,
                      col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = min(1000, n_max));baby.allergy

# data
dat=baby.allergy

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_fa_date=`Service/Charge Date`, baby_fa_icd=`Food Allergy ID9/ICD10`);newdata
unique(newdata$baby_fa_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 1372
length(newdata$part_id)         # 2360
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_fa_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_foodallergy"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_fa_date","baby_fa_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 33

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
rm(baby.allergy, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_clinic_ear                                                
# **************************************************************************** #

# baby_clinic_ear
#-----------------
# rows: 6507
# cols: 3
# unique id: 2849
# repeat: 21
# ICD9/10: "382.9"   "381.3"   "H66.90"  "H66.93"  "H66.91"  "H66.92"  "H93.8X1" "H65.493" "H65.491" "H65.499" "H65.492" "386.30"  "H93.8X3" "H93.8X2"

# read data
baby.ear=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Ear Infection (Clinic)", range = NULL, col_names = TRUE,
                       col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                       guess_max = min(1000, n_max));baby.ear

# data
dat=baby.ear

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_ear_infect_date=`Service/Charge Date`, baby_ear_infect_icd=`Ear Infection ICD9/ICD10`);newdata
unique(newdata$baby_ear_infect_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 2849
length(newdata$part_id)         # 6507
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_ear_infect_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_ear"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_ear_infect_date","baby_ear_infect_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 21

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
rm(baby.ear, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_clinic_eczema                                               
# **************************************************************************** #

# baby_clinic_eczema
#-----------------
# rows: 5026
# cols: 3
# unique id: 1997
# repeat: 36
# ICD9/10: "692.9"  "691.8"  "690.12" "L29.8"  "L30.8"  "L30.9"  NA       "L20.84" "L20.9"  "L23.9"  "L20.89" "L20.83"

# read data
baby.eczema=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Dermatitis-Eczema (Clinic)", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.eczema

# data
dat=baby.eczema

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_eczema_date=`Service/Charge Date`, baby_eczema_icd=`Dermatitis-Eczema ICD9/ICD10`);newdata
unique(newdata$baby_eczema_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 1997
length(newdata$part_id)         # 5026
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_eczema_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_eczema"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_eczema_date","baby_eczema_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 36

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
rm(baby.eczema,dat, newdata,chunks,newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_clinic_dermatitis                                               
# **************************************************************************** #

# baby_clinic_dermatitis
#-----------------
# rows: 607
# cols: 3
# unique id: 487
# repeat: 7
# ICD9/10: "690.12" "690.10" "L21.9"  NA  "L21.1"

# read data
baby.derm=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Seborrheic Dermatitis (Clinic)", range = NULL, col_names = TRUE,
                      col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = min(1000, n_max));baby.derm

# data
dat=baby.derm

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_dermatitis_date=`Service/Charge Date`, baby_dermatitis_icd=`Seborrheic Dermatitis ICD9/ICD10`);newdata
unique(newdata$baby_dermatitis_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 487
length(newdata$part_id)         # 607
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_dermatitis_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_dermatitis"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_dermatitis_date","baby_dermatitis_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 7

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
rm(baby.derm, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                baby_clinic_erythema                                               
# **************************************************************************** #

# baby_clinic_erythema
#-----------------
# rows: 700
# cols: 3
# unique id: 477
# repeat: 10
# ICD9/10: "695.0" "778.8" "L53.0" NA "P83.1"

# read data
baby.tox=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Erythema Toxicum (Clinic)", range = NULL, col_names = TRUE,
                    col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                    guess_max = min(1000, n_max));baby.tox

# data
dat=baby.tox

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_erythema_date=`Service/Charge Date`, baby_erythema_icd=`Erythema Toxicum-ICD9/ICD10`);newdata
unique(newdata$baby_erythema_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 477
length(newdata$part_id)         # 700
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_erythema_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_erythema"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_erythema_date","baby_erythema_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 10

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
rm(baby.tox, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                 baby_clinic_sebaceous                                              
# **************************************************************************** #

# baby_clinic_sebaceous
#-----------------
# rows: 171
# cols: 3
# unique id: 109
# repeat: 8
# ICD9/10: "216.9"  "D22.4"  "D22.9"  "216.4"  "216.3"  "D23.9"  "216.8"  "D22.30" NA

# read data
baby.seb=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Nevus Sebaceous (Clinic)", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.seb

# data
dat=baby.seb

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_sebaceous_date=`Service/Charge Date`, baby_sebaceous_icd=`Nevus Sebaceous ICD9/ICD10`);newdata
unique(newdata$baby_sebaceous_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 109
length(newdata$part_id)         # 171
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_sebaceous_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_sebaceous"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_sebaceous_date","baby_sebaceous_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 8

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
rm(baby.seb, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                 baby_clinic_hemangioma                                              
# **************************************************************************** #

# baby_clinic_hemangioma
#-----------------
# rows: 1035
# cols: 3
# unique id: 473
# repeat: 18
# ICD9/10: "228.00" "D18.00" "757.32" "228.09" "228.01" "448.9"  "Q82.5"  "D18.01" "D18.09" NA "I78.8"

# read data
baby.hem=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Hemangioma (Clinic)", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.hem

# data
dat=baby.hem

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_hemangioma_date=`Service/Charge Date`, baby_hemangioma_icd=`Hemangioma ICD9/ICD10`);newdata
unique(newdata$baby_hemangioma_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 473
length(newdata$part_id)         # 1035
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_hemangioma_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_hemangioma"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_hemangioma_date","baby_hemangioma_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 18

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
rm(baby.hem, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                 baby_obesity_clinic                                              
# **************************************************************************** #

# baby_obesity_clinic
#-----------------
# rows: 490
# cols: 3
# unique id: 316
# repeat: 11
# ICD9/10: "278.00" "V85.54" "Z68.54" "E66.01" "E66.9"  "Z68.53" "V85.53" "E66.09" "E66.3"  "278.02" "278.01"

# read data
baby.ob=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Obesity (Clinic)", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.ob

# data
dat=baby.ob

# rename
newdata=rename(dat, part_id = `Baby Id`, baby_obesity_date=`Service/Charge Date`, baby_obesity_icd=`Obesity ICD9/ICD10`);newdata
unique(newdata$baby_obesity_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 316
length(newdata$part_id)         # 490
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_obesity_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_clinic_obesity"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_obesity_date","baby_obesity_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 11

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
rm(baby.ob, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)