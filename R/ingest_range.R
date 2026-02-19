suppressPackageStartupMessages({
      library(lubridate); library(purrr); library(fs)
})

month_seq <- function(start, end){
      # start & end shoud be in format "YYYY-MM"
      s <- as.Date(paste0(start, '-01'))
      e <- as.Date(paste0(end, '-01'))
      seq(s, e, by='month')
}
   
bts_ingest_month <-function(year, month, overwrite=FALSE){
      csv <- download_bts_month(year, month, out_dir = "data/raw/bts")
      y <- as.integer(year)
      m <- sprintf('%02d', as.integer(month))
      parquet_path <- file.path("data/parquet", sprintf("year=%d",y), sprintf("month=%s",m), "flights.parquet")
      if (!file_exists(parquet_path) || overwrite)
         csv_to_parquet(csv, out_root="data/parquet")
      invisible(parquet_path)
}

weather_ingest_month <- function(year, month, overwrite = FALSE, do_coverage = FALSE){
   out <- file.path("data/raw", sprintf("weather_%d_%02d.parquet", as.integer(year), as.integer(month)))
   if (file_exists(out) && !overwrite)
      return(invisible(out))
   write_weather_month(year, month, out_dir='data/raw', do_coverage=do_coverage)$paths$weather
}

ingest_range <- function(start="2022-01", end="2023-03", overwrite=FALSE, do_coverage=FALSE){
   purrr::walk(month_seq(start,end), function(d){
      y <- year(d)
      m <- month(d)
      bts_ingest_month(y, m, overwrite = overwrite)
      weather_ingest_month(y, m, overwrite = overwrite, do_coverage = do_coverage)
   })
}

