##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        March 14, 2019 
# IRB:
# Description: Cummulative mean function of Abx over time paper #1
# Data: C:\Users\djlemas\Dropbox (UFL)\02_Projects\UFHEALTH\figures
# UPDATE: Modifying sample size.

# **************************************************************************** #
# ***************                Directory Variables           *************** #
# **************************************************************************** #

# Directory Locations
work.dir=paste(Sys.getenv("USERPROFILE"),"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\figures\\",sep="");work.dir
data.dir=paste(Sys.getenv("USERPROFILE"),"\\Dropbox (UFL)\\02_Projects\\UFHEALTH\\figures\\",sep="");data.dir
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
# *****      load data: fig2_V2_RecurrentEventsDataPlot_27Jun19.csv      
# **************************************************************************** # 

# read data
data.file.name="fig2_V2_RecurrentEventsDataPlot_27Jun19.csv";data.file.name
data.file.path=paste0(data.dir,data.file.name);data.file.path
time<- read.csv(data.file.path);time

# diagnostics
dat=time
dim(dat)
names(dat)
str(dat)

# format data
dat$csection=as.factor(dat$csection)

# plot with days on x-axis
#-------------------------
ggplot(dat, aes(x=upper_abx, y=CMF, col=csection)) + geom_line()+
  theme_minimal()+
  xlab("Postnatal Age in Day") + ylab("Mean Antibiotic Episodes")+
  scale_color_discrete(name="Mode of Delivery",labels=c("Vaginal Delivery", "Cesarean Section"))

# convert days to months
dat$months=dat$upper_abx/365

# plot with years on x-axis
#--------------------------
ggplot(dat, aes(x=months, y=CMF, col=csection)) + geom_line()+
  # p+geom_ribbon(aes(ymin=dat$LowerCMF, ymax=dat$UpperCMF), linetype=0.5, alpha=0.001)
  theme_minimal()+
  xlab("Postnatal Age in Years") + ylab("Mean Antibiotic Episodes")+
  scale_color_discrete(name="Mode of Delivery",labels=c("Vaginal Delivery", "Cesarean Section"))
ggsave("fig02_abx_time_V2.png")


