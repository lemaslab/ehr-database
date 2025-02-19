############################################################
##                    convert_gestage_to_total_days
###########################################################

# Function to parse gestational age in weeks and days
parse_gestational_age <- function(gestational_age) {
  if (is.na(gestational_age) || gestational_age == "") {
    return(list(weeks = NA_real_, days = NA_real_))
  }
  
  # Case 1: Gestational age in "weeks days/days" format (e.g., "40 3/7")
  if (grepl("\\d+ \\d+/\\d+", gestational_age)) {
    split_age <- strsplit(gestational_age, " ")[[1]]
    weeks <- as.numeric(split_age[1])
    fractional_days <- strsplit(split_age[2], "/")[[1]]
    days <- round(as.numeric(fractional_days[1]) / as.numeric(fractional_days[2]) * 7)
    return(list(weeks = weeks, days = days))
  }
  
  # Case 2: Gestational age in "weeks" only (e.g., "40")
  if (grepl("^\\d+$", gestational_age)) {
    weeks <- as.numeric(gestational_age)
    days <- 0
    return(list(weeks = weeks, days = days))
  }
  
  # Case 3: Invalid format
  warning(paste("Invalid gestational age format:", gestational_age))
  return(list(weeks = NA_real_, days = NA_real_))
}# END FUNCTION

############################################################
##                    calculate event_gestational_age
###########################################################

# Function to calculate gestational age at the event in total weeks (decimal)
calculate_event_gest_age_weeks_total <- function(gestational_age_weeks_total, date_of_birth, event_date) {
  # Convert inputs to Date type
  date_of_birth <- as.Date(date_of_birth)
  event_date <- as.Date(event_date)
  
  # Calculate the days from the event to the date of birth
  days_to_birth <- as.numeric(date_of_birth - event_date)
  
  # Convert input gestational age in weeks to days
  gestational_age_days_total <- gestational_age_weeks_total * 7
  
  # Calculate gestational age at the event in total days
  gestational_age_at_event_days <- gestational_age_days_total - days_to_birth
  
  # Convert the gestational age at event back to weeks (decimal)
  event_gest_age_weeks_total <- gestational_age_at_event_days / 7
  
  return(event_gest_age_weeks_total)
} # END FUNCTION

############################################################
##                    convert_gest_age
###########################################################

# Function to convert gestational age into weeks and days
# Function to calculate gestational age at a prenatal event
calculate_ga_event <- function(dob, event_date, weeks, days) {
  # Convert input gestational age to total days
  total_days_delivery <- convert_to_total_days(weeks, days)
  
  # Calculate days between delivery and the event
  days_between <- as.numeric(as.Date(dob) - as.Date(event_date))
  
  # Gestational age at event in total days
  total_days_event <- total_days_delivery - days_between
  
  # Convert back to weeks and days
  weeks_event <- total_days_event %/% 7
  days_event <- total_days_event %% 7
  
  # Return gestational age in weeks and days format
  return(paste(weeks_event, "weeks", days_event, "days"))
}

############################################################
##                    convert_to_total_days
###########################################################

# Function to convert gestational age in weeks and days to total days
convert_to_total_days <- function(weeks, days) {
  # Total days = (weeks * 7) + days
  total_days <- (weeks * 7) + days
  return(total_days)
}