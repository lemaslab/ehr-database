##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        Jan 14, 2020
# IRB:
# Description: narrow/broad abx estimates paper #1
# Data: C:\Users\djlemas\Dropbox (UFL)\02_Projects\UFHEALTH\figures
# UPDATE: Modifying sample size.

# **************************************************************************** #
# ***************                Directory Variables           *************** #
# **************************************************************************** #

# Directory Locations
work.dir=paste(Sys.getenv("USERPROFILE"),"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\figures\\",sep="");work.dir
data.dir=paste(Sys.getenv("USERPROFILE"),"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\medication_docs\\",sep="");data.dir
out.dir=paste(Sys.getenv("USERPROFILE"),"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\figures\\",sep="");out.dir

# Set Working Directory
setwd(work.dir)
list.files()

# **************************************************************************** #
# ***************                Library                       *************** #
# **************************************************************************** #

library(readxl)
library(dplyr)
library(ggplot2)

# **************************************************************************** #
# *****      load data: AntibioticPrescriptionsbyYear_16Apr2019.xlsx      
# **************************************************************************** # 

# Frequency Missing = 3817

# read data
data.file.name="AntibioticPrescriptionsbyYear_16Apr2019.xlsx";data.file.name

# file parameters
n_max=1000000

# read data
abx=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "RawDataAntibioticPresc", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));abx

abx$year=as.factor(abx$year)
table(abx$abx_episode)
length(unique(abx$Baby_Id)) # 970

# REPLICATE SUPPLEMENTARY TABLE 2

# select highest abx_episode value WITHIN study ID
new=abx %>%
  group_by(Baby_Id) %>%
  mutate(abx_max=last(abx_episode)) %>%
  distinct(Baby_Id, .keep_all= TRUE)

  # check
  length(unique(new$Baby_Id)) # 970
  table(new$abx_max)          # DONE!
  4024-970
  
# COMPLETE NARROW ANALYSIS

narrow=abx %>%
  select(Baby_Id, year, Medication, classification, abx_time) %>%
  filter(classification=="Narrow") %>%
  group_by(Baby_Id) %>%
  arrange(Baby_Id, abx_time) %>%
  mutate(ticker = row_number(abx_time))
 length(unique(narrow$Baby_Id))

# select highest abx_episode value WITHIN study ID
new.narrow=narrow %>%
  group_by(Baby_Id) %>%
  mutate(abx_narrow_max=last(ticker)) %>%
  distinct(Baby_Id, .keep_all= TRUE)

  # check 
  length(unique(new.narrow$Baby_Id))
  table(new.narrow$abx_narrow_max)          # DONE!
  4024-761
  
# COMPLETE BROAD ANALYSIS
  
  broad=abx %>%
    select(Baby_Id, year, Medication, classification, abx_time) %>%
    filter(classification=="Broad") %>%
    group_by(Baby_Id) %>%
    arrange(Baby_Id, abx_time) %>%
    mutate(ticker = row_number(abx_time))
  length(unique(broad$Baby_Id))
  
  # select highest abx_episode value WITHIN study ID
  new.broad=broad %>%
    group_by(Baby_Id) %>%
    mutate(abx_broad_max=last(ticker)) %>%
    distinct(Baby_Id, .keep_all= TRUE)
  
  # check 
  length(unique(new.broad$Baby_Id))
  table(new.broad$abx_broad_max)          # DONE!
  

