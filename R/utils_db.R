suppressPackageStartupMessages({
      library(DBI)
      library(duckdb)
})

features_parquet_path <- function(year, month){
      file.path('data/features',
                sprintf('features_%d_%02d.parquet',
                        as.integer(year), as.integer(month)))
}
flights_parquet_path <- function(year, month){
      file.path('data/parquet',
                sprintf('year=%d', as.integer(year)),
                sprintf('month=%02d', as.integer(month)),
                'flights.parquet')
}
weather_parquet_path <- function(year, month){
      file.path('data/raw',
                sprintf('weather_%d_%02d.parquet',
                        as.integer(year), as.integer(month)))
}
stations_parquet_path <- function(){
      "data/cache/meteostat_stations.parquet"
}

open_duckdb <- function(read_only=TRUE){
      dir.create('data/duckdb', recursive = TRUE, showWarnings = FALSE)
      dbConnect(duckdb::duckdb(),
                dbdir='data/duckdb/airline.duckdb',
                read_only=read_only)
}
ensure_icu <- function(con){
      invisible(DBI::dbExecute(con, 'INSTALL icu; LOAD icu;'))
}