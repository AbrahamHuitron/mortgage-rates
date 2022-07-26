library(httr) # used for the API calls
library(jsonlite) # used to parse JSON
library(DBI) # used to query the SQL Server
library(odbc) # used to establish ODBC connection
library(config) # used for masking credentials, etc.

# setting a working directory
setwd("~/Projects/R/mortgage-rates")
dsn <- get('sqlserver')

# API key granted by FRED
api_key <- get('fred')

# connection to my local SQL Server
# you must start the server manually prior to connection
con <- dbConnect(
    odbc(),
    Driver = dsn$driver,
    Server = dsn$server,
    Database = dsn$database,
)

# 30 Year Fixed Rate
# querying the API for a 'series' of data
qMORTGAGE30US <- GET(paste0('https://api.stlouisfed.org/fred/series/observations?series_id=MORTGAGE30US&api_key=', api_key, '&file_type=json'))

# getting contents from query
qData <- fromJSON(rawToChar(qMORTGAGE30US$content))

# storing the valuable information in a new variable
MORTGAGE30US <- qData$observations[c('date', 'value')]

# renaming the variables columns for simplicity
colnames(MORTGAGE30US) = c('Date', '30YRFIXED')

# changing the datatypes
MORTGAGE30US$Date <- as.Date(MORTGAGE30US$Date)
MORTGAGE30US$`30YRFIXED` <- as.numeric(MORTGAGE30US$`30YRFIXED`)

# 15 Year Fixed Rate
qMORTGAGE15US <- GET(paste0('https://api.stlouisfed.org/fred/series/observations?series_id=MORTGAGE15US&api_key=', api_key, '&file_type=json'))

qData <- fromJSON(rawToChar(qMORTGAGE15US$content))

MORTGAGE15US <- qData$observations[c('date', 'value')]

colnames(MORTGAGE15US) = c('Date', '15YRFIXED')

MORTGAGE15US$Date <- as.Date(MORTGAGE15US$Date)
MORTGAGE15US$`15YRFIXED` <- as.numeric(MORTGAGE15US$`15YRFIXED`)

# merging the data sets
mortgage_rates <- base::merge(MORTGAGE30US, MORTGAGE15US, by = 'Date', all = TRUE)

# writing to local SQL Server
dbWriteTable(con, "MortgageRates", mortgage_rates, overwrite = TRUE)