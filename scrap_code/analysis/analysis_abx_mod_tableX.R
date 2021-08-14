##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        December 18, 2018 
# IRB:
# Description: Analysis of abx and mode-of-delivery EHR data 
# Data: C:\Users\djlemas\Dropbox (UFL)\02_Projects\UFHEALTH\tables\table03

# **************************************************************************** #
# ***************                Directory Variables           *************** #
# **************************************************************************** #

# Computer
# location="djlemas";location
# location="Dominick";location

# Directory Locations
work.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\tables\\table03\\",sep="");work.dir
data.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\tables\\table03\\",sep="");data.dir
out.dir=paste("C:\\Users\\",location,"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\tables\\",sep="");out.dir

# Set Working Directory
setwd(work.dir)
list.files()

# **************************************************************************** #
# ***************                Library                       *************** #
# **************************************************************************** #

library(readxl)
library(dplyr)

# **************************************************************************** #
# *****      load data: Antibiotic Prescriptions by Year_18dEC18.xlsx       
# **************************************************************************** # 


# read data
n_max=1000000
data.file.name="Antibiotic Prescriptions by Year_18DEC18.xlsx";data.file.name


abx=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "TABLE2", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));abx

# diagnostics
dat=abx
dim(dat)


# abx counts by ATC Groups
dat %>%
  select(Frequency,`ATC-General-lev2`) %>%
  group_by(`ATC-General-lev2`) %>%
  summarize(n = n()) %>%
  mutate(freq= n/sum(n)*100)
  
# abx counts/freq by ATC Groups by age
dat %>%
  select(year,Frequency,`ATC-General-lev2`) %>%
  group_by(year,`ATC-General-lev2`) %>%
  summarize(n = n()) %>%
  mutate(freq= n/sum(n)*100)




mtcars %>%
  group_by(am, gear) %>%
  summarise (n = n()) %>%
  mutate(freq = n / sum(n))


dat.sub=dat.new %>%
  group_by(part_id) %>%
  filter(gest_age_wk>=37, gest_age_wk<=42, baby_birth_wt_gr>2500, is.na(baby_dob)==F, is.na(mod)==F)   

names(dat.sub)
length(unique(dat.sub$part_id)) # 4163

range(dat.sub$gest_age_wk, na.rm=T)
hist(dat.sub$gest_age_wk)
range(dat.sub$baby_birth_wt_gr, na.rm=T) 
hist(dat.sub$baby_birth_wt_gr)
range(dat.sub$days2_baby_meds, na.rm=T)
hist(dat.sub$days2_baby_meds)
table(is.na(dat.sub$baby_dob))
table(dat.sub$mod)
range(dat.sub$abx_episode_broad, na.rm=T)
range(dat.sub$abx_episode_narrow, na.rm=T)
range(dat.sub$abx_episode_total, na.rm=T)

# **************************************************************************** #
# *****      Longitudinal Cutt-points                                              
# **************************************************************************** # 

# subset by 2 WEEK wellness visits
#---------------------------------
df.2wk=dat.sub %>%
  group_by(part_id) %>%
  filter(any(two_wk_well_dummy==T))
length(unique(df.2wk$part_id)) # 3406

# how many participants in analysis?
df.2wk %>%
  group_by(mod) %>%
  summarize(count=n_distinct(part_id),
            abx_mean=mean(abx_episode_total, na.rm=T),
            abx_sd=sd(abx_episode_total, na.rm=T),
            abx_broad_mean=mean(abx_episode_broad, na.rm=T),
            abx_broad_sd=sd(abx_episode_broad, na.rm=T),
            abx_narrow_mean=mean(abx_episode_narrow, na.rm=T),
            abx_narrow_sd=sd(abx_episode_narrow, na.rm=T)) 

# subset by 2 WEEK (1 visit) & 1 YEAR (2+ visit) wellness visits
#---------------------------------
df.one.yr=dat.sub %>%
  group_by(part_id) %>%
  filter(any(two_wk_well_dummy==T & sum(one_year_well_dummy==T, na.rm=T)>=2))
length(unique(df.one.yr$part_id)) # 3094, loss of 312 people

# how many participants in analysis?
df.one.yr %>%
  group_by(mod) %>%
  summarize(count=n_distinct(part_id),
            abx_mean=mean(abx_episode_total, na.rm=T),
            abx_sd=sd(abx_episode_total, na.rm=T),
            abx_broad_mean=mean(abx_episode_broad, na.rm=T),
            abx_broad_sd=sd(abx_episode_broad, na.rm=T),
            abx_narrow_mean=mean(abx_episode_narrow, na.rm=T),
            abx_narrow_sd=sd(abx_episode_narrow, na.rm=T)) 


# subset by 1 MONTH, 1 yr wellness & 2 year wellness
#---------------------------------
df.two.yr=dat.sub %>%
  group_by(part_id) %>%
  filter(any(two_wk_well_dummy==T & sum(one_year_well_dummy==T, na.rm=T)>=1 & sum(two_year_well_dummy==T, na.rm=T)>=1))
length(unique(df.two.yr$part_id)) # 1957, loss of 1449 people

# how many participants in analysis?
df.two.yr %>%
  group_by(mod) %>%
  summarize(count=n_distinct(part_id),
            abx_mean=mean(abx_episode_total, na.rm=T),
            abx_sd=sd(abx_episode_total, na.rm=T),
            abx_broad_mean=mean(abx_episode_broad, na.rm=T),
            abx_broad_sd=sd(abx_episode_broad, na.rm=T),
            abx_narrow_mean=mean(abx_episode_narrow, na.rm=T),
            abx_narrow_sd=sd(abx_episode_narrow, na.rm=T)) 


# subset by 1 MONTH, 1 yr wellness, 2 year wellness, 3 yr wellness
#---------------------------------
df.three.yr=dat.sub %>%
  group_by(part_id) %>%
  filter(any(two_wk_well_dummy==T & sum(one_year_well_dummy==T, na.rm=T)>=1 & sum(two_year_well_dummy==T, na.rm=T)>=1) 
         & sum(three_year_well_dummy==T, na.rm=T)>=1)
length(unique(df.three.yr$part_id)) # 1020

# how many participants in analysis?
df.three.yr %>%
  group_by(mod) %>%
  summarize(count=n_distinct(part_id),
            abx_mean=mean(abx_episode_total, na.rm=T),
            abx_sd=sd(abx_episode_total, na.rm=T),
            abx_broad_mean=mean(abx_episode_broad, na.rm=T),
            abx_broad_sd=sd(abx_episode_broad, na.rm=T),
            abx_narrow_mean=mean(abx_episode_narrow, na.rm=T),
            abx_narrow_sd=sd(abx_episode_narrow, na.rm=T)) 

# **************************************************************************** #
# *****      subset according to medication order (maybe run before episode calc)                                             
# **************************************************************************** # 

# possible data
df.2wk     # 3406
df.one.yr  # 3094
df.two.yr  # 1957
df.three.yr # 1020

# rename
dat.sort=dat.sub

names(dat.sort)
unique(dat.sort$baby_mar_action)
table(dat.sort$baby_mar_action)

# these are codes from "abx_ip" encounters
# [1] <NA>                     GIVEN_RX                 GIVEN                   
# [4] DUE                      MISSED                   HELD                    
# [7] IV STOP                  CANCELED ENTRY           NEW BAG                 
# [10] GIVEN-1ST DOSE EDUCATION START/GIVEN              NEW SYRINGE/CARTRIDGE   
# [13] STOPPED                  GIVEN BY OTHER           SEE ALTERNATIVE  

# NOte: we created "GIVEN_RX" to make sure we get abx_rx (outpatient visits)
dat.new=dat.sort %>%
   filter(baby_mar_action %in% c("GIVEN", "GIVEN BY OTHER", "GIVEN_RX"))

unique(dat.new$baby_mar_action)
length(unique(dat.new$part_id)) # 1853 (1 yr, 124 lost); # 4163 (all data with general criteria)

# **************************************************************************** #
# *****      Summarize the data set: wellness_cats first then wellness.visit second                                              
# **************************************************************************** # 

# create a tibble
dat2_df=tbl_df(dat.new)
dat2_df

# what is status of cs/vaginal
unique(dat2_df$delivery_mode)
table(dat2_df$delivery_mode)

# [1] Vaginal&_Spontaneous_Delivery Vaginal&_Vacuum_(Extractor)  
# [3] C-Section&_Low_Transverse     C-Section&_Low_Vertical      
# [5] C-Section&_Unspecified        Vertical_C-Section           
# [7] Vaginal&_Forceps              C-Section&_Classical         
# [9] VBAC&_Spontaneous             Vaginal&_Breech 
 
names(dat2_df)
# Mean abx_episodes for entire cohort 
summarize(dat2_df, abx_total_mean=mean(abx_episode_total, na.rm=T),
                  abx_broad_mean=mean(abx_episode_broad, na.rm=T),
                  abx_episode_narrow=mean(abx_episode_narrow, na.rm=T)) # 1.94

# Compute variable with highest rank abx_episodes for each participant
# within each wellness visits category
dat3_df=dat2_df %>%
  group_by(part_id, baby_abx_cats) %>%
  mutate(abx_max=last(abx_episode_total)) %>%
  select(part_id,wellness_visit_cats,baby_abx_cats,mod,baby_birth_wt_gr,gest_wk,days2_baby_meds,abx_episode_total,abx_max, days2_baby_wellvisit)

dat3_df$abx_episode_total
head(dat3_df);names(dat3_df)
# dat3_df[1:20,c(1,2,7)]

# WELLNESS CATS
#-------------
#  Descriptive look abx_episode according to wellness-cats and mod
wellness.abx_cats_table02=dat3_df %>%
  group_by(baby_abx_cats, mod) %>% 
  summarise(count = n(),                                   # total count
            count.unique = n_distinct(part_id),            # count of unique participants
            abx_epi_mean=round(mean(abx_episode_total, na.rm=T),2),       # mean abx_episode
            abx_epi_sd=round(sd(abx_episode_total, na.rm=T),2),           # sd abx_episode
            abx_max_mean=round(mean(abx_max, na.rm=T),2),           # mean abx_episode_max
            abx_max_sd=round(sd(abx_max, na.rm=T),2));              # sd abx_episode_max

# make an 'export' variable
wellness.abx_cats_table02$abx_mean_export <- with(wellness.abx_cats_table02, sprintf("%g (%.1f%%)", abx_epi_mean, abx_epi_sd))
# wellness.abx_cats_table02$abx_max_export <- with(wellness.abx_cats_table02, sprintf("%g (%.1f%%)", abx_max_mean, abx_max_sd))

# export
now=Sys.Date(); today=format(now, format="%d%b%y")
write.csv(wellness.abx_cats_table02, file=paste0(out.dir,"table02\\abx_mod_table02_cat",today,".csv"), row.names=F)


# **************************************************************************** #
# *****      Analysis of MOD and abx_episode/ days2_abx                                              
# **************************************************************************** # 

# need to add sd and output as proper table
# need to convert tibble with list to proper data.frame 

# WELLNESS CATS
#-------------
#  ALL TIME POINTS
# t-test: abx_episode and days2_baby_med ~ mod
out.all=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode_total, mod) %>%
  gather(key = variable, value = value, -mod) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="ALL");out.all

#  TIME POINT: 0-6 months 
# t-test: abx_episode and days2_baby_med ~ mod
out.6mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode_total, mod, baby_abx_cats) %>%
  filter(baby_abx_cats=="t.<6mo") %>% 
  gather(key = variable, value = value, -mod, -baby_abx_cats) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.<6mo");out.6mo

#  TIME POINT: t.6_12months 
# t-test: abx_episode and days2_baby_med ~ mod
out.6_12mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode_total, mod, baby_abx_cats) %>%
  filter(baby_abx_cats=="t.6_12mo") %>% 
  gather(key = variable, value = value, -mod, -baby_abx_cats) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.6_12mo");out.6_12mo

#  TIME POINT: t.12_24months 
# t-test: abx_episode and days2_baby_med ~ mod
out.12_24mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode_total, mod, baby_abx_cats) %>%
  filter(baby_abx_cats=="t.12_24mo") %>% 
  gather(key = variable, value = value, -mod, -baby_abx_cats) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.12_24mo");out.12_24mo

#  TIME POINT: t.24_36months 
# t-test: abx_episode and days2_baby_med ~ mod
out.24_36mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode_total, mod, baby_abx_cats) %>%
  filter(baby_abx_cats=="t.24_36mo") %>% 
  gather(key = variable, value = value, -mod, -baby_abx_cats) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.24_36mo");out.24_36mo

#  TIME POINT: t.>36months 
# t-test: abx_episode and days2_baby_med ~ mod
out.36mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode_total, mod, baby_abx_cats) %>%
  filter(baby_abx_cats=="t.>36mo") %>% 
  gather(key = variable, value = value, -mod, -baby_abx_cats) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.>36mo");out.36mo

# combine results into single data.frame
results_well_cats=bind_rows(out.6mo,out.6_12mo,out.12_24mo,out.24_36mo,out.36mo);results_well_cats
results_well_cats.final=results_well_cats %>%
  select(variable,p_value,t_value,mean_csec,mean_vag,time_point)
now=Sys.Date(); today=format(now, format="%d%b%y")
write.csv(results_well_cats.final, file=paste0(out.dir,"table02\\MOD_ABX_table02_wellcats_ttest_",today,".csv"), row.names=F)


# WELLNESS LOTS OF CATS
#----------------------

#  TIME POINT: t.2_wks 
# t-test: abx_episode and days2_baby_med ~ mod
out.2wk=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.2_wks") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.2_wks");out.2wk

#  TIME POINT: t.1_mo 
# t-test: abx_episode and days2_baby_med ~ mod
out.1mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.1_mo") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.1_mo");out.1mo

#  TIME POINT: t.2_mo 
# t-test: abx_episode and days2_baby_med ~ mod
out.2mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.2_mo") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.2_mo");out.2mo

#  TIME POINT: t.4_mo 
# t-test: abx_episode and days2_baby_med ~ mod
out.4mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.4_mo") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.4_mo");out.4mo

#  TIME POINT: t.6_mo 
# t-test: abx_episode and days2_baby_med ~ mod
out.6mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.6_mo") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.6_mo");out.6mo

#  TIME POINT: t.9_mo 
# t-test: abx_episode and days2_baby_med ~ mod
out.9mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.9_mo") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.9_mo");out.9mo

#  TIME POINT: t.12_mo 
# t-test: abx_episode and days2_baby_med ~ mod
out.12mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.12_mo") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.12_mo")

#  TIME POINT: t.15_mo (sig.dif) # DONE
# t-test: abx_episode and days2_baby_med ~ mod
out.15mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.15_mo") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.15_mo")

#  TIME POINT: t.18_mo (sig.dif) # DONE
# t-test: abx_episode and days2_baby_med ~ mod
out.18mo=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.18_mo") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.18_mo")

#  TIME POINT: t.2_yr (sig.dif) # DONE
# t-test: abx_episode and days2_baby_med ~ mod
out.2yr=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.2_yr") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.2_yr")

#  TIME POINT: t.2.5_yr (sig.dif) # DONE
# t-test: abx_episode and days2_baby_med ~ mod
out.2_5yr=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.2.5_yr") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.2.5_yr")

#  TIME POINT: t.3_yr (sig.dif) # DONE
# t-test: abx_episode and days2_baby_med ~ mod
out.3yr=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.3_yr") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.3_yr")

#  TIME POINT: t.4_yr (sig.dif) # DONE
# t-test: abx_episode and days2_baby_med ~ mod
out.4yr=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.4_yr") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.4_yr")

#  TIME POINT: t.5_yr (sig.dif) # DONE
# t-test: abx_episode and days2_baby_med ~ mod
out.5yr=dat3_df %>% ungroup() %>%
  select(days2_baby_meds, abx_episode, mod, wellness.visit) %>%
  filter(wellness.visit=="t.5_yr") %>% 
  gather(key = variable, value = value, -mod, -wellness.visit) %>%
  group_by(mod, variable) %>% 
  summarise(value = list(value)) %>% 
  spread(mod, value) %>% 
  group_by(variable) %>% 
  mutate(p_value = round(t.test(unlist(`c-section`), unlist(vaginal))$p.value,4),
         t_value = round(t.test(unlist(`c-section`), unlist(vaginal))$statistic,2),
         mean_csec = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[1],2),
         mean_vag = round(t.test(unlist(`c-section`), unlist(vaginal))$estimate[2],2),
         time_point="t.5_yr");out.5yr

# combine results into single data.frame
results_well_cont=bind_rows(out.all,out.2wk,out.1mo,out.2mo,out.4mo,out.6mo,out.9mo,
                  out.12mo,out.15mo,out.18mo,out.2yr,out.2_5yr,out.3yr,
                  out.4yr,out.5yr);results_well_cont

# combine results into single data.frame
results_well_cont.final=results_well_cont %>%
  select(variable,p_value,t_value,mean_csec,mean_vag,time_point)
now=Sys.Date(); today=format(now, format="%d%b%y")
write.csv(results_well_cont.final, file=paste0(out.dir,"MOD_ABX_well_cont_ttest_",today,".csv"), row.names=F)
newobject=xtable(results_well_cont.final)
print.xtable(newobject, type="latex", file="test_01May18.tex")