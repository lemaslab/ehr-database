##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:             Dominick Lemas 
# START  Date:        June 27, 2019
# REVISE Date:        January 21, 2020
# IRB:                IRB201601899:UFHealth Early Life Exposures and Pediatric Outcomes	
# Description: REVISE: Count of Abx per medication type for paper #1
# Data: C:\Users\djlemas\Dropbox (UFL)\02_Projects\UFHEALTH\figures

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
library(forcats)

# **************************************************************************** #
# *****   REVISED SUBMISSION:   load data: fig1_infant_abx_count_V3_21Jan20.csv  
# **************************************************************************** # 

# https://stackoverflow.com/questions/34913196/create-stack-bar-with-counts-and-fill-group-levels-in-the-same-graph
# https://plotnine.readthedocs.io/en/stable/tutorials/miscellaneous-show-counts-and-percentages-for-bar-plots.html

# read data
data.file.name="fig1_infant_abx_count_V3_21Jan20.csv";data.file.name
data.file.path=paste0(data.dir,data.file.name);data.file.path
counts<- read.csv(data.file.path);counts

# need to create figure 1

# diagnostics
dat=counts
dim(dat)
str(dat)

# format data

dat.n=dat %>% 
  mutate(class_order=ordered(classification, levels = c("total", "narrow", "broad")),
         abx_episode=as.factor(abx_episode)) %>%
  mutate(abx_episode_4=fct_collapse(abx_episode, 
                               zero="0", 
                               one="1", 
                               two="2",
                               three="3", 
                               four="4")) %>%
  mutate(abx_percent=round((frequency/4024)*100, 1)) %>%
  mutate(abx_per_char=format(abx_percent, nsmall = 1)) %>%
  mutate(percent_count=paste0(abx_per_char,"% (",frequency,")"))

# plot
ggplot(dat.n, aes(x = classification, y = frequency, fill = abx_episode, label = frequency)) +
  geom_bar(stat = "identity") +
  geom_text(size = 3, position = position_stack(vjust = 0.5))


# plot abx episode number by participant count number.
ggplot(data=dat.n, aes(x=abx_episode_4, y=as.numeric(abx_percent))) +
  geom_bar(stat="identity")+
  geom_text(aes(label=abx_percent), vjust=-0.3, size=3.5)+
  theme_minimal()+
  scale_y_continuous(limits=c(0, 100))+
  xlab("Number of Antibiotic Episodes") + ylab("Percentage")
g + theme_minimal() + scale_y_continuous(limits=c(0, 100)) + xlab("Number of Antibiotic Episodes") + ylab("Percentage")





# **************************************************************************** #
# *****   FIRST SUBMISSION:   load data: fig1_infant_abx_count_V1_14Mar19.csv  
# **************************************************************************** # 

# read data
data.fgsave("fig1_infant_abx_count_V3.png")
list.files()

plot <- ggplot(dat.n, aes(abx_episode_4, frequency, fill=class_order))
plot <- plot + geom_bar(stat = "identity", position = 'dodge') + geom_text(aes(label=frequency), vjust=-0.3, size=3.5)
plot <- plotile.name="fig1_infant_abx_count_V1_14Mar19.csv";data.file.name
data.file.path=paste0(data.dir,data.file.name);data.file.path
counts<- read.csv(data.file.path);counts

# diagnostics
dat=counts
dim(dat)

# revise to include fewer categories
dat$abx_episode=as.factor(dat$abx_episode)

dat$abx_episode_4=fct_collapse(dat$abx_episode, 
                               zero="0", 
                               one="1", 
                               two="2",
                               three="3", 
                               four=c("4","5","6","7","8","9","10","11","12"))
levels(dat$abx_episode_4)
# order levels
df1=dat%>%
  mutate(abx_episode_4 = factor(abx_episode_4, 
                                levels = c("zero",
                                           "one", 
                                           "two", 
                                           "three",
                                           "four")))%>%
  group_by(abx_episode_4) %>%
  summarize(percent_4 = format(round(sum(percent),1),nsmall=1))%>%
  mutate(abx_episode_4=fct_recode(abx_episode_4, "0"="zero", "1"="one","2"="two",
                                  "3"="three","4+"="four"))


# quick check
sum(as.numeric(df1$percent_4)) # 99.9

# plot abx episode number by participant count number.
ggplot(data=df1, aes(x=abx_episode_4, y=as.numeric(percent_4))) +
  geom_bar(stat="identity")+
  geom_text(aes(label=percent_4), vjust=-0.3, size=3.5)+
  theme_minimal()+
  scale_y_continuous(limits=c(0, 100))+
  xlab("Number of Antibiotic Episodes") + ylab("Percentage")
ggsave("fig1_infant_abx_count_V3.png")
list.files()

