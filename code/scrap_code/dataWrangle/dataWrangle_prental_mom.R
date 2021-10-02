##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        April 23, 2018 
# IRB:
# Description: Data management for link mom-baby data extracted from RedCap. 
# Data: C:\Users\Dominick\Dropbox (UFL)\IRB\UF\UFHealth\redcap_export
# Analysis: Curate data for maternal prenatal visits

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

#Read Data
data.file.name="UFHealth_EHR_MomBaby_Prenatal_16Jan18.csv";data.file.name
data.file.path=paste0(data.dir,"\\",data.file.name);data.file.path
ufhealth.mod<- read.csv(data.file.path);ufhealth.mod

# look at data
dat=ufhealth.mod
head(dat); str(dat); names(dat)

dat3=dat %>%
  select(part_id,redcap_repeat_instrument,redcap_repeat_instance,mom_id, mom_race_link, mom_ethnicity_link, mom_prenat_apt_date_link,
         mom_prenat_ht_link, mom_prenat_wt_oz_link, mom_prenat_enc_type_link, days2_prenatal_apt_link, 
         mom_prenat_ht_inch_link, mom_prenat_wt_lb_link) %>% 
  mutate(mom_prenat_ht_link.save=mom_prenat_ht_link) %>%
  filter(redcap_repeat_instrument %in% c("linked_mom_prenatal_apt")) %>%
  separate(mom_prenat_ht_link, c("mom_ht_ft","mom_ht_in"), "_") %>%

# drop characters
dat3$mom_ht_ft=as.numeric(gsub("'","", dat3$mom_ht_ft))
dat3$mom_ht_in=as.numeric(gsub("'","", dat3$mom_ht_in))

# compute height in inches
dat4=dat3 %>% 
  mutate(mom_ht_meter=((mom_ht_ft*12)+mom_ht_in)*0.0254) %>%
  mutate(mom_wt_kg=mom_prenat_wt_lb_link*0.453592) %>%
  mutate(mom_bmi=mom_wt_kg/mom_ht_meter^2)

# plot
hist(dat4$days2_prenatal_apt_link)
hist(dat4$mom_bmi)

