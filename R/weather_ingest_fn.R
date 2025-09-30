# functionalized version of R/weather_ingest.R
suppressPackageStartupMessages({
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
})

write_weather_month <- function(
            year,
            month,
            flights_parquet = NULL,
            stations_cache = "data/cache/meteostat_stations.parquet",
            out_dir = "data/raw",
            do_coverage = FALSE,
            duckdb_path = "data/duckdb/airline.duckdb"
) {
      mm <- sprintf("%02d", as.integer(month))
      if (is.null(flights_parquet)) {
            flights_parquet <- file.path("data/parquet", sprintf("year=%d", year), sprintf("month=%02d", month), "flights.parquet")
      }
      if (!file.exists(flights_parquet)) {
            stop("Flights parquet not found: ", flights_parquet)
      }
      
      dir.create(dirname(stations_cache), showWarnings = FALSE, recursive = TRUE)
      dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
      
      # 1. Airports present in BTS parquet (IATA)
      flights  <- arrow::open_dataset(flights_parquet)
      iata_vec <- flights %>% distinct(Origin) %>% collect() %>% pull()
      
      # 2. IATA -> ICAO crosswalk (OpenFlights)
      airports_xwalk <- read.csv(
            "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
            header = FALSE, stringsAsFactors = FALSE
      ) %>%
            transmute(iata = V5, icao = V6) %>%
            filter(iata %in% iata_vec, icao != "\\N")
      
      # 3. Load or cache Meteostat stations dictionary
      stations_url <- "https://bulk.meteostat.net/v2/stations/full.json.gz"
      if (!file.exists(stations_cache)) {
            gz_tmp <- tempfile(fileext = ".json.gz")
            RETRY("GET", stations_url,
                  timeout(300),
                  write_disk(gz_tmp, overwrite = TRUE),
                  times = 4, pause_min = 2, pause_cap = 8)
            stations_dict <- jsonlite::fromJSON(gzfile(gz_tmp, open = "rb"), flatten = TRUE) %>% as_tibble()
            write_parquet(stations_dict, stations_cache)
            unlink(gz_tmp)
      } else {
            stations_dict <- read_parquet(stations_cache) %>% as_tibble()
      }
      
      # 4. IATA -> Meteostat station_id via ICAO 
      crosswalk <- airports_xwalk %>%
            left_join(
                  stations_dict %>% transmute(icao = `identifiers.icao`, meteostat_id = id),
                  by = "icao",
                  na_matches = "never"
            ) %>%
            filter(!is.na(meteostat_id))
      
      if (nrow(crosswalk) == 0) stop("No matching Meteostat stations for airports in this month.")
      
      # 5. Fetch hourly weather for each station for the target month
      target_year  <- as.integer(year)
      target_month <- as.integer(month)
      
      get_station_weather <- function(station_id) {
            url <- sprintf("https://data.meteostat.net/hourly/%d/%s.csv.gz", target_year, station_id)
            weather_raw <- read_csv(
                  url,
                  col_types = cols(
                        year  = col_integer(),
                        month = col_integer(),
                        day   = col_integer(),
                        hour  = col_integer(),
                        .default = col_double(),
                        coco  = col_integer()
                  ),
                  show_col_types = FALSE
            )
            
            needed  <- c("dwpt","rhum","prcp","snow","wdir","wspd","wpgt","pres","tsun","temp")
            missing <- setdiff(needed, names(weather_raw))
            weather_raw[missing] <- NA_real_
            
            weather_raw %>%
                  mutate(
                        time = make_datetime(year, month, day, hour, tz = "UTC"),
                        station_id = station_id
                  ) %>%
                  filter(month(time) == target_month) %>%
                  select(time, station_id, all_of(needed), coco)
      }
      
      station_ids <- unique(crosswalk$meteostat_id)
      weather <- purrr::map_dfr(station_ids, get_station_weather)
      
      # 6. Persist Parquets
      weather_path <- file.path(out_dir, sprintf("weather_%d_%s.parquet", target_year, sprintf("%02d", target_month)))
      xwalk_path   <- file.path(out_dir, "airport_station_xwalk.parquet")
      
      write_parquet(weather, weather_path)
      write_parquet(crosswalk, xwalk_path)
      
      message("Wrote weather: ", weather_path)
      message("Wrote xwalk:   ", xwalk_path)
      
      # 7. Optional: coverage check in DuckDB (read-only joins, no writes) ---
      if (isTRUE(do_coverage)) {
            dir.create(dirname(duckdb_path), showWarnings = FALSE, recursive = TRUE)
            con <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = FALSE)
            on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
            DBI::dbExecute(con, "INSTALL icu; LOAD icu;")
            
            DBI::dbExecute(con, sprintf("CREATE OR REPLACE VIEW flights AS SELECT * FROM parquet_scan('%s');", flights_parquet))
            DBI::dbExecute(con, sprintf("CREATE OR REPLACE VIEW weather AS SELECT * FROM parquet_scan('%s');", weather_path))
            DBI::dbExecute(con, sprintf("CREATE OR REPLACE VIEW xwalk   AS SELECT * FROM parquet_scan('%s');", xwalk_path))
            
            DBI::dbExecute(con, "
      CREATE OR REPLACE VIEW flights_ext AS
      SELECT
        *,
        date_trunc(
          'hour',
          strptime(FlightDate || ' ' || lpad(CRSDepTime::VARCHAR, 4, '0'), '%Y-%m-%d %H%M')
        ) AS crs_dep_hour
      FROM flights;
    ")
            
            coverage <- DBI::dbGetQuery(con, "
      SELECT COUNT(*) AS total_flights,
             COUNT(w.temp) AS with_weather
      FROM flights_ext f
      LEFT JOIN xwalk x
        ON f.Origin = x.iata
      LEFT JOIN weather w
        ON x.meteostat_id = w.station_id
       AND (f.crs_dep_hour AT TIME ZONE 'UTC') = w.time;
    ")
            print(coverage)
      }
      
      invisible(list(paths = list(weather = weather_path, xwalk = xwalk_path)))
}
