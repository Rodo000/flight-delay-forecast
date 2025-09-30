# R/03_build_features_fn.R
suppressPackageStartupMessages({
   library(DBI)
   library(duckdb)
})

# build features for one year/month , write parquet, return path
build_features_month <- function(year, month) {
   MM <- sprintf("%02d", as.integer(month))
   
   # paths
   flights_path  <- flights_parquet_path(year, month)
   weather_path  <- weather_parquet_path(year, month)
   stations_path <- stations_parquet_path()
   out_parquet   <- features_parquet_path(year, month)
   
   con <- open_duckdb(read_only = FALSE)
   on.exit({ try(DBI::dbExecute(con, "CHECKPOINT;"), silent = TRUE)
      DBI::dbDisconnect(con, shutdown = TRUE) }, add = TRUE)
   ensure_icu(con)
   
   # register parquet as views
   DBI::dbExecute(con, sprintf("CREATE OR REPLACE VIEW flights AS SELECT * FROM parquet_scan('%s');", flights_path))
   DBI::dbExecute(con, sprintf("CREATE OR REPLACE VIEW weather AS SELECT * FROM parquet_scan('%s');", weather_path))
   DBI::dbExecute(con, sprintf("CREATE OR REPLACE VIEW meteostat_stations AS SELECT * FROM parquet_scan('%s');", stations_path))
   
   # airports (IATA/ICAO/tz) limited to origins in this month; exclude bad tz '\N'
   airports_df <- read.csv(
      "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
      header = FALSE, stringsAsFactors = FALSE
   )
   airports_tz <- subset(airports_df, V5 != "\\N" & V6 != "\\N" & V12 != "\\N" & nzchar(V12))[, c(5, 6, 12)]
   names(airports_tz) <- c("iata", "icao", "tz")
   
   iata_in_month <- DBI::dbGetQuery(con, "SELECT DISTINCT Origin AS iata FROM flights")$iata
   airports_tz   <- subset(airports_tz, iata %in% iata_in_month)
   
   DBI::dbWriteTable(con, "airports_tz_raw", airports_tz, overwrite = TRUE)
   DBI::dbExecute(con, "
    CREATE OR REPLACE TABLE airports_tz AS
    SELECT iata, icao, tz
    FROM airports_tz_raw
    WHERE tz IS NOT NULL AND tz <> '' AND tz <> '\\N'
  ")
   
   # station map via ICAO
   DBI::dbExecute(con, "
    CREATE OR REPLACE TABLE airport_station_map AS
    SELECT a.iata, a.icao, a.tz, s.id AS station_id
    FROM airports_tz a
    JOIN meteostat_stations s
      ON lower(s.\"identifiers.icao\") = lower(a.icao)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.iata ORDER BY s.id) = 1
  ")
   
   # Features table (±30 min nearest hour weather); pass through the 'needed' weather fields
   DBI::dbExecute(con, sprintf("
    CREATE OR REPLACE TABLE features_%d_%s AS
    WITH f AS (
      SELECT
        fl.Year, fl.Month, fl.DayofMonth,
        fl.IATA_CODE_Reporting_Airline AS carrier,
        fl.Origin AS origin,
        fl.Dest   AS dest,
        map.station_id AS station_id,
        make_timestamptz(
          CAST(fl.Year AS BIGINT),
          CAST(fl.Month AS BIGINT),
          CAST(fl.DayofMonth AS BIGINT),
          CAST(floor(CAST(TRIM(fl.CRSDepTime) AS BIGINT)/100) AS BIGINT),
          CAST(CAST(TRIM(fl.CRSDepTime) AS BIGINT) %% 100 AS BIGINT),
          0.0::DOUBLE,
          map.tz
        ) AS sched_dep_tstz,
        fl.DepDelay AS dep_delay_min
      FROM flights fl
      JOIN airport_station_map map ON map.iata = fl.Origin
      WHERE fl.CRSDepTime IS NOT NULL
        AND (fl.Cancelled IS NULL OR fl.Cancelled = 0)
        AND map.tz IS NOT NULL AND map.tz <> '' AND map.tz <> '\\N'
    ),
    fx AS (
      SELECT
        *,
        date_trunc('hour', sched_dep_tstz) AS dep_hr_bin,
        date_part('hour', sched_dep_tstz)  AS dep_hour_local,
        date_part('dow',  sched_dep_tstz)  AS dow
      FROM f
    ),
    wx AS (
      SELECT
        station_id,
        CAST(time AS TIMESTAMPTZ) AS wx_ts,
        dwpt, rhum, prcp, snow, wdir, wspd, wpgt, pres, tsun, temp, coco
      FROM weather
    )
    SELECT
      fx.carrier, fx.origin, fx.dest,
      fx.sched_dep_tstz,
      fx.dep_hour_local, fx.dow,
      wx.dwpt, wx.rhum, wx.prcp, wx.snow, wx.wdir, wx.wspd, wx.wpgt, wx.pres, wx.tsun, wx.temp, wx.coco,
      fx.dep_delay_min,
      (fx.dep_delay_min >= 15) AS late_15
    FROM fx
    JOIN wx
      ON wx.station_id = fx.station_id
     AND wx.wx_ts BETWEEN fx.dep_hr_bin - INTERVAL '30' MINUTE
                      AND fx.dep_hr_bin + INTERVAL '30' MINUTE
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY fx.carrier, fx.origin, fx.dest, fx.sched_dep_tstz
        ORDER BY ABS(date_diff('second', wx.wx_ts, fx.dep_hr_bin))
      ) = 1
  ", as.integer(year), MM))
   
   dir.create("data/features", recursive = TRUE, showWarnings = FALSE)
   DBI::dbExecute(con, sprintf("
    COPY (SELECT * FROM features_%d_%s)
    TO '%s' (FORMAT PARQUET);
  ", as.integer(year), MM, out_parquet))
   
   message("Built: ", out_parquet)
   return(invisible(out_parquet))
}
