##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        June 19, 2018 
# IRB:
# Description: Table 1: abx and mode-of-delivery EHR data 
# Data: C:\Users\djlemas\Dropbox (UFL)\02_Projects\UFHEALTH\RedCap\rdata

# **************************************************************************** #
# ***************                Directory Variables           *************** #
# **************************************************************************** #

# Computer
location="djlemas";location

# Directory Locations
work.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\rdata\\",sep="");work.dir
data.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\RedCap\\rdata\\",sep="");data.dir
out.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\tables\\",sep="");out.dir

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
library(tidyverse)

# **************************************************************************** #
# *****      l             load data: abx_mod_19Jun18.rdata       
# **************************************************************************** # 

load(file="abx_mod_19Jun18.rdata")
head(dat7)
names(dat7)

# quick diagnostics
length(unique(dat7$part_id)) # 7667

# **************************************************************************** #
# ***** subset the data: gest_age (wk) >37 & <42, birth_wt (gr) >2500                                               
# **************************************************************************** # 

# check before subsetting
range(dat7$gest_age_wk, na.rm=T)        # 23.14286 102.28571 wks
range(dat7$baby_birth_wt_gr, na.rm=T)   # 420 6567 grams
length(unique(dat7$part_id)) # 7667 people

dat3=dat7 %>%
  group_by(part_id) %>%
  filter(gest_age_wk>=37, gest_age_wk<=42, baby_birth_wt_gr>2500, is.na(baby_dob)==F, is.na(mod)==F)   

# check
range(dat3$gest_age_wk)
range(dat3$baby_birth_wt_gr)
length(unique(dat3$part_id)) # 4163 people

# **************************************************************************** #
# *****      Longitudinal Cutt-points                                              
# **************************************************************************** # 

# subset by 2 WEEK wellness visits
#---------------------------------
df.2wk=dat3 %>%
  group_by(part_id) %>%
  filter(any(two_wk_well_dummy==T))
length(unique(df.2wk$part_id)) #3406 people

# subset by 2 WEEK (1 visit) & 1 YEAR (2+ visit) wellness visits
#---------------------------------
df.one.yr=dat3 %>%
  group_by(part_id) %>%
  filter(any(two_wk_well_dummy==T & sum(one_year_well_dummy==T, na.rm=T)>=2))
length(unique(df.one.yr$part_id)) # 3094 people

# subset by 2 WEEK (1 visit), 1 yr wellness & 2 year wellness
#---------------------------------
df.two.yr=dat3 %>%
  group_by(part_id) %>%
  filter(any(two_wk_well_dummy==T & sum(one_year_well_dummy==T, na.rm=T)>=1 & sum(two_year_well_dummy==T, na.rm=T)>=1))
length(unique(df.two.yr$part_id)) # 1957 people

# subset by 2 WEEK (1 visit), 1 yr wellness, 2 year wellness, 3 yr wellness
#---------------------------------
df.three.yr=dat3 %>%
  group_by(part_id) %>%
  filter(any(two_wk_well_dummy==T & sum(one_year_well_dummy==T, na.rm=T)>=1 & sum(two_year_well_dummy==T, na.rm=T)>=1) 
         & sum(three_year_well_dummy==T, na.rm=T)>=1)
length(unique(df.three.yr$part_id)) # 1020


# **************************************************************************** #
# *****      formatt/subset the data: single observations                                              
# **************************************************************************** # 

# all data for table 01 is contained in redcap_repeat_instance=1

# create table
names(dat3)
length(unique(dat3$part_id)) # 4163 people
range(dat3$redcap_repeat_instance)

dat4=dat3 %>%
  filter(redcap_repeat_instance==1)
length(unique(dat4$part_id)) # 4163 people

# check
range(dat4$gest_age_wk)
range(dat4$baby_birth_wt_gr)
length(unique(dat4$part_id)) # 4163 people

head(dat4)
names(dat4)
dat4$abx_episode_total

# https://stackoverflow.com/questions/34587317/using-dplyr-to-create-summary-proportion-table-with-several-categorical-factor-v



# Categorical data: ALL
#-----------------
length(unique(dat4$part_id))
dat5 <- melt(dat4, measure.vars=c("baby_race","baby_ethnicity","mom_ethnicity_link", "mom_race_link","abx_episode_total","abx_episode_narrow","abx_episode_broad","wellness_visit_cats","baby_abx_cats"))
length(unique(dat4$part_id))
res.all <- dat5 %>%
  select(part_id,variable,value) %>%
  group_by(variable, value) %>%
  na.omit() %>%
  summarise (n = n_distinct(part_id)) %>%
  mutate(freq = n / sum(n));res.all

#make an 'export' variable
res.all$export <- with(res.all, sprintf("%i (%.1f%%)", n, freq*100))

# export
now=Sys.Date(); today=format(now, format="%d%b%y")
write.csv(res.all, file=paste0(out.dir,"abx_mod_table01_cat_ALL_",today,".csv"), row.names=F)


# Categorical data: MOD
#-----------------

length(unique(dat4$part_id))
dat5 <- melt(dat4, measure.vars=c("baby_race","baby_ethnicity","mom_ethnicity_link", "mom_race_link","abx_episode_total","abx_episode_narrow","abx_episode_broad","wellness_visit_cats","baby_abx_cats"))
res <- dat5 %>%
  select(part_id,mod,variable,value) %>%
  group_by(mod, variable, value) %>%
  na.omit() %>%
  summarise (n = n_distinct(part_id)) %>%
  mutate(freq = n / sum(n));res

#
res <- dat5 %>%
  select(part_id,mod,variable,value) %>%
  group_by(mod) %>%
  na.omit() %>%
  summarise (n = n_distinct(part_id)) # cs: 1225, vg: 2938

#make an 'export' variable
res$export <- with(res, sprintf("%i (%.1f%%)", n, freq*100))

#reshape again
output <- dcast(variable+value~mod, value.var="export", data=res, fill="missing") #use drop=F to prevent silent missings 
#'silent missings'
output$variable <- as.character(output$variable)
#make 'empty lines' 
empties <- data.frame(variable=unique(output$variable), stringsAsFactors=F)
empties[,colnames(output)[-1]] <- ""

#bind them together
output2 <- rbind(empties,output)
output2 <- output2[order(output2$variable,output2$value),]

#optional: 'remove' variable if value present

output2$variable[output2$value!=""] <- ""

# export table: categorical
#-------------

now=Sys.Date(); today=format(now, format="%d%b%y")
write.csv(output, file=paste0(out.dir,"abx_mod_table01_cat_",today,".csv"), row.names=F)

#-----------------
# Continuous data: ALL
#-----------------
names(dat4)
length(unique(dat4$part_id))
dat6 <- melt(dat4, measure.vars=c("days2_baby_meds","baby_birth_wt_gr","gest_age_wk", "baby_nicu_los","abx_episode_total","abx_episode_narrow","abx_episode_broad"))
length(unique(dat6$part_id))

res.all=dat6 %>%
  group_by(variable) %>%
  select(variable,value) %>%
  summarize(mean=mean(value, na.rm=T), sd=sd(value, na.rm=T));res.all

#make an 'export' variable
res.all$export <- with(res.all, sprintf("%g (%.1f%%)", mean, sd))

# export table: continuous
#-------------

now=Sys.Date(); today=format(now, format="%d%b%y")
write.csv(res.all, file=paste0(out.dir,"abx_mod_table01_cont_ALL_",today,".csv"), row.names=F)


#-----------------
# Continuous data: MOD
#-----------------
names(dat4)
dat6 <- melt(dat4, measure.vars=c("days2_baby_meds","baby_birth_wt_gr","gest_age_wk", "baby_nicu_los","abx_episode_total","abx_episode_narrow","abx_episode_broad"))

res=dat6 %>%
  group_by(mod,variable) %>%
  select(mod,variable,value) %>%
  summarize(mean=mean(value, na.rm=T), sd=sd(value, na.rm=T));res

#make an 'export' variable
res$export <- with(res, sprintf("%g (%.1f%%)", mean, sd))

#reshape again
output <- dcast(variable~mod, value.var="export", data=res, fill="missing") #use drop=F to prevent silent missings 
#'silent missings'
output$variable <- as.character(output$variable)
#make 'empty lines' 
empties <- data.frame(variable=unique(output$variable), stringsAsFactors=F)
empties[,colnames(output)[-1]] <- ""

#bind them together
output2 <- rbind(empties,output)
output2 <- output2[order(output2$variable),]

# export table: continuous
#-------------

now=Sys.Date(); today=format(now, format="%d%b%y")
write.csv(output2, file=paste0(out.dir,"abx_mod_table01_cont_",today,".csv"), row.names=F)

# t-test
#-------------
#  ALL TIME POINTS
out.all=dat6 %>% ungroup() %>%
  select(variable, value, mod) %>%
  filter(!value=="NA", !mod=="NA", !variable=="abx_episode_total") %>%
  #gather(key = variable, value = value, -mod) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="ALL") %>%
  select(variable,p_value, t_value, mean_csec, mean_vag, time_point)

# export table: continuous t-tes
#-------------
write.csv(out.all, file=paste0(out.dir,"abx_mod_table01_cont_ttest_",today,".csv"),row.names=F)
