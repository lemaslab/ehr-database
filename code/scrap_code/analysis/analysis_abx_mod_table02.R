##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        December 18, 2018 
# IRB:
# Description: Analysis of abx data EHR data 
# Data: C:\Users\djlemas\Dropbox (UFL)\02_Projects\UFHEALTH\tables\table03

# **************************************************************************** #
# ***************                Directory Variables           *************** #
# **************************************************************************** #

# Computer
location="djlemas";location

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
library(ggplot2)

# **************************************************************************** #
# *****      load data: Antibiotic Prescriptions by Year_18dEC18.xlsx       
# **************************************************************************** # 

# read data
n_max=1000000
data.file.name="Antibiotic Prescriptions by Year_18dEC18.xlsx";data.file.name


abx=read_xlsx(paste(data.dir,data.file.name,sep=""), sheet = "TABLE2", range = NULL, col_names = TRUE,
                   col_types = NULL, na = "NA", trim_ws = TRUE, skip = 0, n_max = Inf,
                   guess_max = min(1000, n_max));abx

# diagnostics
dat=abx
dim(dat)

# how many participants
length(unique(dat$Baby_Id))  # 970 infants with at least 1 abx
df=dat%>%
  select(Baby_Id, abx_episode)%>%
  group_by(abx_episode, Baby_Id)%>%
  count() 

# how many unique abx-episodes: 1-12 in 970 people
unique(df$abx_episode)
length(unique(df$Baby_Id))
table(df$abx_episode)
hist(table(df$abx_episode))

# need ggplot histogram for paper. 4,024 in denominator.
ggplot(data=df, aes(x=abx_episode)) +
  geom_histogram(fill="#880011", binwidth=1, bin=12) + 
  stat_bin(binwidth = 1, col = "black")   

ggplot(data=df, aes(x=factor(df$abx_episode))) + 
  geom_bar() +
  geom_text(stat='count', aes(label=..count..), vjust=-1) +
  xlab("Number of Antibiotic Episodes") + ylab("Number of Infants")

library(forcats)

# revise
df$abx_episode=as.character(df$abx_episode)
levels(df$abx_episode)
df$abx_episode_4=fct_collapse(df$abx_episode, one="1",two="2",three="3", four=c("4","5","6","7","8","9","10","11","12"))
levels(df$abx_episode_4)
# order levels
df1=df%>%
mutate(abx_episode_4 = factor(abx_episode_4, 
                             levels = c("one", 
                                        "two", 
                                        "three",
                                        "four")))

ggplot(data=df1, aes(x=factor(df1$abx_episode_4))) + 
geom_bar() +
geom_text(stat='count', aes(label=..count..), vjust=-1) +
xlab("Number of Antibiotic Episodes") + ylab("Number of Infants")
       
970/4024 # 0.24105
240/4024 # 0.05964

# 1     2   3   4   5   6   7   8   9  10  11  12 
# 970 240  79  37  18  10   7   6   2   2   2   2


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


# https://cran.r-project.org/web/packages/qwraps2/vignettes/summary-statistics.html

library(qwraps2)

# define the markup language we are working in.
# options(qwraps2_markup = "latex") is also supported.
options(qwraps2_markup = "markdown")


Let’s build a list-of-lists to pass to the summaries argument of summary_table. 
The inner lists are named formulae defining the wanted summary. 
These formulae are passed through dplyr::summarize_ to generate the table. 
The names are important, as they are used to label row groups and row names 
in the table.

# Level 2 comparisons
#--------------------

dat.table=dat %>%
  select(year, Frequency,`ATC-General-lev2`) %>%
  group_by(`ATC-General-lev2`,year)

our_summary1 <-
  list("ATC-code" =
         list("J01A" = ~ qwraps2::n_perc0(`ATC-General-lev2` == "J01A"),
              "J01C" = ~ qwraps2::n_perc0(`ATC-General-lev2` == "J01C"),
              "J01D" = ~ qwraps2::n_perc0(`ATC-General-lev2` == "J01D"),
              "J01E" = ~ qwraps2::n_perc0(`ATC-General-lev2` == "J01E"),
              "J01F" = ~ qwraps2::n_perc0(`ATC-General-lev2` == "J01F"),
              "J01X" = ~ qwraps2::n_perc0(`ATC-General-lev2` == "J01X"),
              "J04A" = ~ qwraps2::n_perc0(`ATC-General-lev2` == "J04A")
              )
  )

summary_table(dat.table, our_summary1)
summary_table(dplyr::group_by(dat.table, year), our_summary1)

# Level 3 comparisons
#--------------------

dat.table=dat %>%
  select(year, Frequency,`ATC-General-lev3`) %>%
  group_by(`ATC-General-lev3`,year)

our_summary1 <-
  list("ATC-code" =
         list("J01CA" = ~ qwraps2::n_perc0(`ATC-General-lev3` == "J01CA"),
              "J01CE" = ~ qwraps2::n_perc0(`ATC-General-lev3` == "J01CE")
         )
  )

summary_table(dat.table, our_summary1)
summary_table(dplyr::group_by(dat.table, year), our_summary1)

# export markdown. Then convert to html. bring into word and instructions below
# on how to convert to tables.
# http://www.schussman.com/article/convert-latex-tables-to-word

