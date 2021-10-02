make some noise

##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        January 31, 2018 
# IRB:
# Description: Data management for link mom-baby data extracted from RedCap. 
# Data: C:\Users\Dominick\Dropbox (UFL)\IRB\UF\UFHealth\redcap_export
# Analysis: Curate data for mode of delivery and infant outcomes

# **************************************************************************** #
# ***************                Directory Variables           *************** #
# **************************************************************************** #

# Computer
# location="djlemas";location
# location="Dominick";location

# Directory Locations
work.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\redcap_export\\",sep="");work.dir
data.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\redcap_export\\",sep="");data.dir
out.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\redcap_export\\",sep="");out.dir

# Set Working Directory
setwd(work.dir)
list.files()

# **************************************************************************** #
# ***************                Library                       *************** #
# **************************************************************************** #

# library(readxl)
library(data.table)
library(tidyr)
library(dplyr)
library(reshape2)

# **************************************************************************** #
# ***************      UFHealth_EHR_MomBaby_Prenatal_16Jan18.csv                                              
# **************************************************************************** # 

# UFHealth_EHR_MomBaby_Prenatal_16Jan18.csv
#-----------------
# rows: 
# cols:  
# unique id: 
# repeat: 
# ICD9/10: 

#Read Data
data.file.name="UFHealth_EHR_MomBaby_Prenatal_16Jan18.csv";data.file.name
data.file.path=paste0(data.dir,"\\",data.file.name);data.file.path
ufhealth.mod<- read.csv(data.file.path);ufhealth.mod

# look at data
dat=ufhealth.mod
head(dat); str(dat); names(dat)

# limit to just a few variables
dat.s=dat[,c("part_id","delivery_mode","baby_birth_wt_gr")]
str(dat.s)

# subset to include only babies 
test=is.na(dat.s$delivery_mode);test[1:30]
which(is.na(dat.s$baby_birth_wt_gr))
newdata <- subset(dat.s, is.na(dat.s$delivery_mode)=='FALSE' & is.na(dat.s$baby_birth_wt_gr)=="FALSE")
head(newdata)

# format data
newdata$part_id=as.character(newdata$part_id)
newdata$delivery_mode=as.character(newdata$delivery_mode)
str(newdata)

# recode variables
unique(newdata$delivery_mode) # how many unique entries for MOD; 24 (below)

# [1]* "Vaginal&_Spontaneous_Delivery"   
# [2]* "C-Section&_Low_Transverse"       
# [3]* "Vaginal&_Vacuum_(Extractor)"     
# [4]* "C-Section&_Unspecified"         
# [5]* "Vaginal&_Forceps"                 
# [6]* "Vertical_C-Section"              
# [7]* "C-Section&_Low_Vertical"         
# [8]* "NOT_INCLUDED_IN_ORIGINAL_SOURCE"
# [9]* "VBAC&_Spontaneous"               
# [10]* "C-Section&_Classical"            
# [11]* "Vaginal&_Breech"                 
# [12]* "Extramural_Delivery"            
# [13]* "Other" 

# condense down to binary (vaginal/c-section)
newdata$mod=NA
newdata$mod=ifelse(newdata$delivery_mode=="Vaginal&_Spontaneous_Delivery","vaginal",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="C-Section&_Low_Transverse","c-section",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="Vaginal&_Vacuum_(Extractor)","vaginal",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="C-Section&_Unspecified","c-section",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="Vaginal&_Forceps","vaginal",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="Vertical_C-Section","vaginal",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="C-Section&_Low_Vertical","c-section",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="NOT_INCLUDED_IN_ORIGINAL_SOURCE",NA,newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="VBAC&_Spontaneous","vaginal",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="C-Section&_Classical","c-section",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="Vaginal&_Breech","vaginal",newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="Extramural_Delivery",NA,newdata$mod)
newdata$mod=ifelse(newdata$delivery_mode=="Other",NA,newdata$mod)
head(newdata) # did recode work? yes
unique(newdata$mod)

# what does distribution of c-section vs vaginal delivery
table(newdata$mod)
# c-section   vaginal total 
# 3500 (0.3672)      6029 (0.6327)      9529

# what is distribution of un-coded deliveries
table(newdata$delivery_mode)

# analysis
newdata$mod1=ifelse(newdata$mod=="vaginal",1,newdata$mod)
newdata$mod1=ifelse(newdata$mod=="c-section",2,newdata$mod1)
newdata$mod1=as.factor(newdata$mod1)
head(newdata)
t.test(newdata$mod1, newdata$baby_birth_wt_gr, na.action=na.omit)

hist(newdata$baby_birth_wt_gr)
boxplot(newdata$baby_birth_wt_gr~newdata$mod1)

### need to consider gestational age, LOS in NICU

head(newdata)

str(newdata)

label(data$baby_race)="baby_race"
label(data$baby_ethnicity)="baby_ethnicity"
label(data$baby_dob)="baby_dob"
label(data$baby_birth_wt_gr)="baby_birth_wt_gr"
label(data$delivery_mode)="delivery_mode"
label(data$baby_gest_age)="baby_gest_age"
label(data$baby_nicu_los)="baby_nicu_los"
label(data$days2_baby_admit)="days2_baby_admit"
label(data$baby_obs_date)="baby_obs_date"
label(data$baby_ht_cm)="baby_ht_cm"
label(data$baby_wt_kgs)="baby_wt_kgs"
label(data$baby_head_circ_cm)="baby_head_circ_cm"
label(data$days2_baby_wellvisit)="days2_baby_wellvisit"
label(data$baby_ht1_date)="baby_ht1_date"
label(data$days2_baby_ht1)="days2_baby_ht1"
label(data$baby_ht1_cm)="baby_ht1_cm"
label(data$baby_hc1_date)="baby_hc1_date"
label(data$days2_baby_hc1)="days2_baby_hc1"
label(data$baby_hc1_cm)="baby_hc1_cm"
label(data$mom_id)="mom_id2"
label(data$mom_race_link)="mom_race_link"
label(data$mom_ethnicity_link)="mom_ethnicity_link"
label(data$mom_prenat_apt_date_link)="mom_prenat_apt_date_link"
label(data$mom_prenat_ht_link)="mom_prenat_ht_link"
label(data$mom_prenat_wt_oz_link)="mom_prenat_wt_oz_link"
label(data$mom_prenat_enc_type_link)="mom_prenat_enc_type_link"
label(data$days2_prenatal_apt_link)="days2_prenatal_apt_link"
label(data$mom_prenat_ht_inch_link)="mom_prenat_ht_inch_link"
label(data$mom_prenat_wt_lb_link)="mom_prenat_wt_lb_link"
#Setting Units


