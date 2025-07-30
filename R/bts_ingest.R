library(glue)
library(fs)
library(vroom)
library(arrow)

# monthly download url
bts_url <- function(year, month){
      mm <- sprintf('%02d', month)
      glue("https://www.transtats.bts.gov/DownLoad_Table.asp?",
           "Table_ID=236&Has_Group=3&YEAR={year}&MONTH={mm}"
      )
}

# download + unzip
download_bts_month <- function(year, month,
                               out_dir='data/raw/bts'){
      dir_create(out_dir, recurse = TRUE)
      mm <- sprintf("%02d", month)
      zip <- path(out_dir, glue("OnTime_{year}_{mm}.zip"))
      if (!file_exists(zip)){
            download.file(bts_url(year, month), zip, mode = 'wb')
      }
      csv <- path(out_dir,
                  glue("On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.csv"))
      if (!file_exists(csv)) unzip(zip, exdir = out_dir)
      csv
}

# convert csv to parquet
csv_to_parquet <- function(csv_path,
                           out_root='data/parquet'){
      df <- vroom(csv_path, delim=',', show_col_types = FALSE)
      yr <- df$YEAR[1]
      mo <- sprintf('%02d', df$MONTH[1])
      out <- path(out_root, glue("year={yr}/month={mo}/flights.parquet"))
      dir_create(path_dir(out), recurse = TRUE)
      write_parquet(df, out, compression = 'snappy')
      invisible(out)
}
