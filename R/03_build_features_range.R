suppressPackageStartupMessages({
      library(lubridate)
      library(purrr)
      library(fs)
})

month_seq <- function(start, end){
      s <- as.Date(paste0(start, "-01"))
      e <- as.Date(paste0(end, "-01"))
      seq(s, e, by="month")
}

# builds one month, optional skip & overwrite 
build_features_month_safe <- function(year, month, overwrite=FALSE){
      out <- features_parquet_path(year, month)
      
      if (file_exists(out) && !overwrite){
            message("Skipping (exists): ", out)
            return(invisible(out))
      }
      if (file_exists(out) && overwrite){
            file_delete(out)
      }
      build_features_month(year, month)
}

build_features_range <- function(start="2022-01", end="2023-03", overwrite=FALSE){
   walk(month_seq(start, end), function(d){
      build_features_month_safe(year(d), month(d), overwrite = overwrite)
   })
}

