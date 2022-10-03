``{r, message=FALSE}

# **************************************************************************** #
# ***************                Library                       *************** #
# **************************************************************************** #

library(keyringr)
library(tidyverse)
library(redcapAPI)
library(REDCapR)
library(dplyr)
```

```{r, message=FALSE}
# Windows
source("~/ehr-database/code/utils/utils.R")
api_token=get_API_token("redcap_ehr2")

# API and URL
uri='https://redcap.ctsi.ufl.edu/redcap/api/'
rcon <- redcapConnection(url=uri, token=api_token)

# import data
result=importRecords(
  rcon,
  import_ready,
  overwriteBehavior = "overwrite",
  returnContent = c('count','ids','nothing'),
  returnData = FALSE,
  logfile = "log.txt",
  batch.size=1000)

test=REDCapR::redcap_write(
  import_ready,
  batch_size = 100L,
  interbatch_delay = 0.5,
  continue_on_error = FALSE,
  uri,
  api_token,
  verbose = TRUE,
  config_options = NULL
)


```
