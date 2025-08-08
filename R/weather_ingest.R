library(dplyr)
library(arrow)
library(lubridate)
library(data.table)
library(DBI)
library(duckdb)
library(purrr)
library(jsonlite)
library(httr)
library(readr)

# airports present in BTS parquet (IATA codes)
flights  <- open_dataset("data/parquet/year=2023/month=01/flights.parquet")
iata_vec <- flights %>% distinct(Origin) %>% collect() %>% pull()

# iata -> icao cross-walk 
airports_xwalk <- read.csv(
      "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
      header = FALSE, stringsAsFactors = FALSE
) %>%
      transmute(
            iata = V5,
            icao = V6
      ) %>%
      filter(iata %in% iata_vec, icao != "\\N")

# download & cache Meteostat full station list
stations_url <- "https://bulk.meteostat.net/v2/stations/full.json.gz"
stations_cache <- "data/cache/meteostat_stations.parquet"
dir.create("data/cache", showWarnings = FALSE, recursive = TRUE)

if (!file.exists(stations_cache)) {
      gz_tmp <- tempfile(fileext = ".json.gz")
      
      RETRY("GET", stations_url,
            timeout(300),
            write_disk(gz_tmp, overwrite = TRUE),
            times = 4, pause_min = 2, pause_cap = 8)
      
      stations_dict <- jsonlite::fromJSON(
            gzfile(gz_tmp, open = "rb"), flatten = TRUE) %>%
            as_tibble()
      write_parquet(stations_dict, stations_cache)
      unlink(gz_tmp)
} else {
      stations_dict <- read_parquet(stations_cache) %>% as_tibble()
}

# iata -> meteostat id cross-walk via icao
crosswalk <- airports_xwalk %>%
      left_join(
            stations_dict %>%
                  transmute(
                        icao = identifiers.icao,
                        meteostat_id = id
                  ),
            by = "icao",
            na_matches = "never"
      ) %>%
      filter(!is.na(meteostat_id))

if (nrow(crosswalk) == 0)
      stop("No matching Meteostat stations for the airports in this month.")

# fetch weather for that month for each station
target_year  <- 2023
target_month <- 1

get_station_weather <- function(station_id) {
      url <- sprintf("https://data.meteostat.net/hourly/%d/%s.csv.gz",
                     target_year, station_id)
      
      weather_raw <- read_csv(
        url,
        col_types = cols(
          year  = col_integer(),
          month = col_integer(),
          day   = col_integer(),
          hour  = col_integer(),
          .default = col_double(),   # grab all numeric cols that exist
          coco  = col_integer()      # cloud-code is integer
        ),
        show_col_types = FALSE
      )
      
      needed <- c("dwpt", "rhum", "prcp", "snow", "wdir",
                  "wspd", "wpgt", "pres", "tsun", "temp")
      missing <- setdiff(needed, names(weather_raw))
      weather_raw[missing] <- NA_real_
      
      weather_parsed <- weather_raw %>%
            mutate(
                  time = make_datetime(year, month, day, hour, tz = "UTC"),
                  station_id = station_id        
            ) %>%
            filter(month(time) == target_month) %>%   
            select(time, station_id, all_of(needed), coco)
      
      weather_parsed
}

weather <- map_dfr(unique(crosswalk$meteostat_id), get_station_weather)

# persist Parquets
dir.create("data/raw", showWarnings = FALSE, recursive = TRUE)
write_parquet(weather, "data/raw/weather_2023_01.parquet")
write_parquet(crosswalk, "data/raw/airport_station_xwalk.parquet")

# duckdb: views & coverage check
dir.create("data/duckdb", showWarnings = FALSE, recursive = TRUE)
con <- dbConnect(duckdb::duckdb(), "data/duckdb/airdelay.duckdb", read_only = FALSE)
dbExecute(con, 'INSTALL icu; LOAD icu;' )

dbExecute(con, "
  CREATE OR REPLACE VIEW flights AS
  SELECT * FROM parquet_scan('data/parquet/year=2023/month=01/flights.parquet');
")

dbExecute(con, "
  -- build timestamp column rounded to the nearest hour
  CREATE OR REPLACE VIEW flights_ext AS
  SELECT
    *,
    date_trunc(
      'hour',
      strptime(
        FlightDate || ' ' || lpad(CRSDepTime::VARCHAR, 4, '0'),
        '%Y-%m-%d %H%M'
      )
    ) AS crs_dep_hour
  FROM flights;
")

dbExecute(con, "
  CREATE OR REPLACE VIEW weather AS
  SELECT
    station_id,
    time AS wx_ts,
    temp, dwpt, rhum,
    prcp, snow,
    wdir, wspd, wpgt,
    pres, tsun, coco
  FROM parquet_scan('data/raw/weather_2023_01.parquet');
")

dbExecute(con, "
  CREATE OR REPLACE VIEW xwalk AS
  SELECT *
  FROM parquet_scan('data/raw/airport_station_xwalk.parquet');
")

coverage <- dbGetQuery(con, "
  SELECT COUNT(*) AS total_flights,
         COUNT(w.temp) AS with_weather
  FROM flights_ext f
  LEFT JOIN xwalk x ON f.Origin = x.iata
  LEFT JOIN weather w ON x.meteostat_id = w.station_id
                      AND (f.crs_dep_hour AT TIME ZONE 'UTC') = w.wx_ts;
")

print(coverage)

