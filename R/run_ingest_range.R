suppressPackageStartupMessages({
      library(lubridate)
      library(purrr)
      library(fs)
})

source("R/bts_ingest.R") #download_bts_month()
source("R/weather_ingest_fn.R") #write_weather_month()
source("R/ingest_range.R")

ingest_range(
      start="2022-01",
      end="2023-03",
      overwrite=FALSE,
      do_coverage=FALSE
)
