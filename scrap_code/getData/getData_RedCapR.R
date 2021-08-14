##-------------- 
# **************************************************************************** #
# ***************                Project Overview              *************** #
# **************************************************************************** #

# Author:      Dominick Lemas 
# Date:        March 22, 2019 
# IRB:
# Description: RedCap API system for UFHealth EHR project
# Data: C:\Users\djlemas\Dropbox (UFL)\02_Projects\UFHEALTH\figures

# **************************************************************************** #
# ***************                Library                       *************** #
# **************************************************************************** #

library(keyringr)
library(redcapAPI)

# Get Redcap API Token
# # https://cran.r-project.org/web/packages/keyringr/vignettes/Avoiding_plain_text_passwords_in_R_with_keyringr.html
credential_label <- "redcap_ehr"
credential_path <- paste(Sys.getenv("USERPROFILE"), '\\DPAPI\\passwords\\', Sys.info()["nodename"], '\\', credential_label, '.txt', sep="")


uri <- "https://redcap.ctsi.ufl.edu/redcap/api/"
token <-decrypt_dpapi_pw(credential_path)
print(token)

# Create connections
rcon <- redcapConnection(url=uri, token=token)

# list of instruments
exportInstruments(rcon)
