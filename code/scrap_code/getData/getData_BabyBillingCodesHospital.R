##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #
           
# Author:      Dominick Lemas 
# Date:        January 05, 2018 
# IRB:
# Description:Import infant clinic data to RedCap from flat files. 
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
# ***************                Baby-Billing Codes (Hospital).xlsx                                              
# **************************************************************************** #      

# file parameters
n_max=10000
data.file.name="Baby-Billing Codes (Hospital).xlsx";data.file.name

# **************************************************************************** #
# ***************                 baby_hospital_asthma                                              
# **************************************************************************** #


# baby_hospital_asthma
#-----------------
# rows: 5293
# cols: 3
# unique id: 2465
# repeat: 29
# ICD9/10: "J069"   "J45909" "4659"   "49390"  "49391"  "49392"  "J45998" "J4541"  "J45901" "J4530"  "49300"  "49302"  "J4531"  "4660"   "J45902" "J4540"  "J4542"  "49320" 
#          "49382"  "J4521"  "J4520"  "J4522"  "J4550"  "J4551"  "J209"   "49121"  "J4552"  "J4532"  "J449"   "J45991" "4658"   "49310"  "49311"  "49322"  "49312"

# read data
baby.asthma=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Asthma", range = NULL, col_names = TRUE,
                      col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = min(1000, n_max));baby.asthma
# data
dat=baby.asthma

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_asthma_hosp_date=`Admit Date`, baby_asthma_hosp_icd=`ICD9/ICD10 Code`);newdata
unique(newdata$baby_asthma_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 2465
length(newdata$part_id)         # 5293
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_asthma_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_asthma"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_asthma_hosp_date","baby_asthma_hosp_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 29

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
# ***************                 baby_hospital_dermatitis                                              
# **************************************************************************** #

# baby_hospital_dermatitis
#-----------------
# rows: 687
# cols: 3
# unique id: 455
# repeat: 17
# ICD9/10: 6929"  "L309"  "L298"  "6918"  "L308"  "L209"  "L2089" "L2083" "L2084" "L239"  "69012"

# read data
baby.derm=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Seborrheic Dermatitis", range = NULL, col_names = TRUE,
                      col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = min(1000, n_max));baby.derm

# data
dat=baby.derm

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_derm_hosp_date=`Admit Date`, baby_derm_hosp_icd=`ICD9/ICD10 Code`);newdata
unique(newdata$baby_derm_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 455
length(newdata$part_id)         # 687
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_derm_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_dermatitis"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_derm_hosp_date","baby_derm_hosp_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 17

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
# ***************                 baby_hospital_ear                                              
# **************************************************************************** #

# baby_hospital_ear
#-----------------
# rows: 2548
# cols: 3
# unique id: 1669
# repeat: 28
# ICD9/10: "3829"   "H6690"  "3813"   "H6692"  "H6693"  "H6691"  "H65491" "H65493" "H938X2" "H65499" "H65492" "H938X3"

# read data
baby.ear=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Ear Infection", range = NULL, col_names = TRUE,
                    col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                    guess_max = min(1000, n_max));baby.ear

# data
dat=baby.ear;dat

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_ear_hosp_date=`Admit Date`, baby_ear_hosp_icd=`ICD9/ICD10 Code`);newdata
unique(newdata$baby_ear_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 1669
length(newdata$part_id)         # 2548
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_ear_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_ear"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_ear_hosp_date","baby_ear_hosp_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 28

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
# ***************                 baby_hospital_eczema                                              
# **************************************************************************** #

# baby_hospital_eczema
#-----------------
# rows: 2548
# cols: 3
# unique id: 1669
# repeat: 28
# ICD9/10: "3829"   "H6690"  "3813"   "H6692"  "H6693"  "H6691"  "H65491" "H65493" "H938X2" "H65499" "H65492" "H938X3"

# read data
baby.eczema=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Dermatitis-Eczema", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.eczema

# data
dat=baby.eczema;dat

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_eczema_hosp_date=`Admit Date`, baby_eczema_hosp_icd=`ICD9/ICD10 Code`);newdata
unique(newdata$baby_eczema_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 1669
length(newdata$part_id)         # 2548
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_eczema_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_eczema"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_eczema_hosp_date","baby_eczema_hosp_icd","redcap_repeat_instrument"))  
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 28

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
rm(baby.eczema, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                 baby_hospital_foodallergy                                              
# **************************************************************************** #

# baby_hospital_foodallergy
#-----------------
# rows: 1339
# cols: 3
# unique id: 980
# repeat: 17
# ICD9/10: "78791"   "V1501"   "V1505"   "Z91010"  "7080"    "Z91018"  "9989"    "L500"    "9957"    "Z9102"   "T781XXA" "5583"    "V5889"   "K5229"

# read data
baby.food=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Food Allergy", range = NULL, col_names = TRUE,
                      col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = min(1000, n_max));baby.food

# data
dat=baby.food;dat

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_fa_hosp_date=`Admit Date`, baby_fa_hosp_icd=`ICD9/ICD10 Code`);newdata
unique(newdata$baby_fa_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 980
length(newdata$part_id)         # 1339
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_fa_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_foodallergy"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_fa_hosp_date","baby_fa_hosp_icd","redcap_repeat_instrument"))  
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
rm(baby.food, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                 baby_hospital_hemangioma                                              
# **************************************************************************** #

# baby_hospital_hemangioma
#-----------------
# rows: 533
# cols: 4
# unique id: 511
# repeat: 7
# ICD9/10: "22801" "75732" "22809" "Q825"  "22800" "D1801" "D1800" "D1809"

# read data
baby.hemang=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Hemangioma", range = NULL, col_names = TRUE,
                    col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                    guess_max = min(1000, n_max));baby.hemang

# data
dat=baby.hemang;dat

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_hemang_hosp_date=`Admit Date`, baby_hemang_hosp_icd=`ICD9/ICD10 Code`);newdata
newdata.1=newdata[,c(1,3,4)]
newdata=newdata.1
unique(newdata$baby_hemang_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 511
length(newdata$part_id)         # 533
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_hemang_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_hemangioma"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_hemang_hosp_date","baby_hemang_hosp_icd","redcap_repeat_instrument"))  
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
rm(baby.hemang, dat, newdata,chunks, newdata.1, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                 baby_hospital_sebaceous                                              
# **************************************************************************** #

# baby_hospital_sebaceous
#-----------------
# rows: 31
# cols: 3
# unique id: 31  
# repeat: 1
# ICD9/10: "2168"  "2169"  "2163"  "D224"  "2164"  "D229"  "D2239"

# read data
baby.seb=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Nevus sebaceous", range = NULL, col_names = TRUE,
                      col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                      guess_max = min(1000, n_max));baby.seb

# data
dat=baby.seb;dat

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_sebaceous_hosp_date=`Admit Date`, baby_sebaceous_hosp_icd=`ICD9/ICD10 Code`);newdata
unique(newdata$baby_sebaceous_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 31
length(newdata$part_id)         # 31
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_sebaceous_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_sebaceous"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_sebaceous_hosp_date","baby_sebaceous_hosp_icd","redcap_repeat_instrument"))  
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
rm(baby.seb, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                 baby_hospital_obesity                                              
# **************************************************************************** #

# baby_hospital_obesity
#-----------------
# rows: 30 
# cols: 3
# unique id: 26
# repeat: 2
# ICD9/10: "27800" "E669"  "Z6854" "E6601" "V8554" "Z6853" "27801" "V8553"

# read data
baby.ob=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Obesity", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));baby.ob

# data
dat=baby.ob;dat

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_obesity_hosp_date=`Admit Date`, baby_obesity_hosp_icd=`ICD9/ICD10 Code`);newdata
unique(newdata$baby_obesity_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 26
length(newdata$part_id)         # 30
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_obesity_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_obesity"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_obesity_hosp_date","baby_obesity_hosp_icd","redcap_repeat_instrument"))  
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
rm(baby.ob, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                 baby_hospital_erythema                                              
# **************************************************************************** #

# baby_hospital_erythema
#-----------------
# rows: 1037
# cols: 3
# unique id: 1032
# repeat: 2
# ICD9/10: "7788" "6950" "P831" "L530"

# read data
baby.tox=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Erythema toxicum", range = NULL, col_names = TRUE,
                  col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                  guess_max = min(1000, n_max));baby.tox

# data
dat=baby.tox;dat

# rename
newdata=rename(dat, part_id = `Baby-Id`, baby_erythema_hosp_date=`Admit Date`, baby_erythema_hosp_icd=`ICD9/ICD10 Code`);newdata
unique(newdata$baby_erythema_hosp_icd)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 1032
length(newdata$part_id)         # 1037
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$baby_erythema_hosp_date,format='%Y-%m-%d')),]
names(newdata2); head(newdata2)

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="baby_hospital_erythema"
names(newdata3); head(newdata3)

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            
setkeyv(dt, c("part_id", "baby_erythema_hosp_date","baby_erythema_hosp_icd","redcap_repeat_instrument"))  
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
rm(baby.tox, dat, newdata,chunks, newdata2, newdata3, dt, dt3, dt4, dt5)
