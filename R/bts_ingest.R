library(glue)
library(fs)
library(vroom)
library(arrow)
library(curl)

# url builder
bts_url <- function(year, month) {
   glue(
      "https://transtats.bts.gov/PREZIP/",
      "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_",
      "{year}_{month}.zip"
   )
}

# downloader
download_bts_month <- function(year, month,
                               out_dir = "data/raw/bts") {
   
   dir_create(out_dir, recurse = TRUE)
   mm  <- sprintf("%02d", month)                         
   zip <- path(out_dir, glue("OnTime_{year}_{mm}.zip"))
   
   if (!file_exists(zip)) {
      message("Downloading...", basename(zip))
      
      h <- curl::new_handle()
      curl::handle_setopt(h, ssl_verifypeer = FALSE, ssl_verifyhost = FALSE)
      
      curl::curl_download(
         url      = bts_url(year, month),
         destfile = zip,
         mode     = "wb",
         handle   = h
      )
   }
   
   csv <- path(
      out_dir,
      glue(
         "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_",
         "{year}_{month}.csv"
      )
   )
   if (!file_exists(csv))
      unzip(zip, exdir = out_dir)
   
   csv
}

# csv to parquet
csv_to_parquet <- function(csv_path,
                           out_root = "data/parquet") {
   
   df <- vroom(csv_path, delim = ",", show_col_types = FALSE)
   yr <- df$Year[1]
   mo <- sprintf("%02d", df$Month[1])
   
   out <- path(out_root, glue("year={yr}/month={mo}/flights.parquet"))
   dir_create(path_dir(out), recurse = TRUE)
   write_parquet(df, out, compression = "snappy")
   invisible(out)
}
