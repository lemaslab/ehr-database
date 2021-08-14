##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #
           
# Author:      Dominick Lemas 
# Date:        January 06, 2018 
# IRB:
# Description: Import maternal prenatal visit data to to redcap from flat files. 
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
# library(plyr)

# **************************************************************************** #
# ***************                Mom Prenatals.xlsx                                              
# **************************************************************************** #      

# file parameters
n_max=10000
data.file.name="Mom Prenatals.xlsx";data.file.name

# **************************************************************************** #
# ***************                mom_demography                                              
# **************************************************************************** #

# mom_demography
#-----------------
# rows: 13853
# cols: 3
# unique id: 13853
# repeat: 1
# ICD9/10: NA

# read data
mom.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Mom", range = NULL, col_names = TRUE,
          col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
          guess_max = min(1000, n_max));mom.dat

# data
dat=mom.dat

# rename
newdata=rename(dat, part_id=`Mom-Id`, mom_race=Race, mom_ethnicity=Ethnicity)
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 13853
length(newdata$part_id)         # 13853
names(newdata); head(newdata)

# sort (not needed in this case)
newdata2 <- newdata
head(newdata2)
  
# redcap_repeat_instrument
newdata2$redcap_repeat_instrument="mom_demography"

# redcap_repeat_instance
newdata2$redcap_repeat_instance=1

# redcap_event_name
newdata2$redcap_event_name="visit_1_arm_1"
dt3=newdata2

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
data.file.name.export=as.character(mom.dat.FINAL[2,2]);data.file.name.export

chunks=split(mom.dat.FINAL, floor(0:(nrow(mom.dat.FINAL)-1)/batchSize))
for (i in 1:length(chunks))
  { # second loop
  write.table(chunks[[i]],paste0(out.dir,data.file.name.export,i,'.csv'),row.names=F, sep="\t")
  } # end second loop

# clear slate
rm(mom.dat, dat, newdata, newdata2, newdata3, dt, dt3, dt4, dt5)

# **************************************************************************** #
# ***************                mom_prenatal_apt                                               
# **************************************************************************** #

# mom_prenatal_apt
#-----------------
# rows: 109221
# cols: 5
# unique id: 9239
# repeat: 113
# ICD9/10: NA

# read data
prenat.dat=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "Prenatals by Appt", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));prenat.dat

# data
dat=prenat.dat

# rename
newdata=rename(dat, part_id = `Mom ID`, mom_prenat_apt_date= `Appt Time`, mom_prenat_enc_type= `Enc Type`,mom_prenat_ht=Height, mom_prenat_wt_oz=Weight)
names(newdata); head(newdata)

# unique ID? Some moms had multiple babies in data set
length(unique(newdata$part_id)) # 
length(newdata$part_id)         # 
names(newdata); head(newdata)

# sort
newdata2 <- newdata[order(newdata$part_id, as.Date(newdata$mom_prenat_apt_date,format='%Y-%m-%d %H:%M:%S')),]

# redcap_repeat_instrument
newdata3=newdata2
newdata3$redcap_repeat_instrument="mom_prenatal_apt"

# create "redcap_repeat_instance" variable
dt <- as.data.table(newdata3)            # Create data.table
setkeyv(dt, c("part_id", "mom_prenat_apt_date","mom_prenat_ht","mom_prenat_wt_oz","mom_prenat_enc_type","redcap_repeat_instrument"))  # Create key for data.table
dt3 <- dt[, redcap_repeat_instance := seq_len(.N), by = "part_id"]        # Create new variable
head(dt3); range(dt3$redcap_repeat_instance) 
max(unique(dt3$redcap_repeat_instance)) # 113

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
dt4$tmp=dt4$mom_prenat_ht
dt4$tmp=gsub("'"," ",dt4$tmp)
dt4$tmp=gsub('"'," ",dt4$tmp)
dt4$tmp=trimws(dt4$tmp, "b") 
dt4$tmp=gsub('  '," ",dt4$tmp)
dt4$mom_prenat_ht_inch=sapply(strsplit(as.character(dt4$tmp)," "), function(x){12*as.numeric(x[1]) + as.numeric(x[2])})
dt4=dt4[,-c("tmp")]

# compute "mom_prenat_wt_lb"
dt4$mom_prenat_wt_lb=dt4$mom_prenat_wt_oz/16

# format "mom_prenat_ht"
dt4$mom_prenat_ht=paste0("&",dt4$mom_prenat_ht,"&")
dt4$mom_prenat_ht=gsub(" ","_",dt4$mom_prenat_ht) 
head(dt4)
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
rm(prenat.dat, dat, newdata, newdata2, newdata3, dt, dt3, dt4, dt5)