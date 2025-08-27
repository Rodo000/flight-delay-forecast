#register Parquet, map airport->station, join flights->weather, build features

YEAR <- 2023L
MONTH <- 1L
MM <- sprintf("%02d", MONTH)

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

# paths
duckdb_dir <- "data/duckdb"
duckdb_path <- file.path(duckdb_dir, "airline.duckdb")
flights_path <- file.path("data/parquet", paste0("year=", YEAR), paste0("month=", MM), "flights.parquet")
weather_path <- file.path("data/raw", paste0("weather_", YEAR, "_", MM, ".parquet"))
stations_path <- "data/cache/meteostat_stations.parquet"

dir.create(duckdb_dir, recursive = TRUE, showWarnings = FALSE)
con <- dbConnect(duckdb::duckdb(), dbdir = duckdb_path, read_only = FALSE)

# tz
dbExecute(con, "INSTALL icu; LOAD icu;")

# register parquet
dbExecute(con, sprintf("CREATE OR REPLACE VIEW flights AS  SELECT * FROM parquet_scan('%s');", flights_path))
dbExecute(con, sprintf("CREATE OR REPLACE VIEW weather AS  SELECT * FROM parquet_scan('%s');", weather_path))
dbExecute(con, sprintf("CREATE OR REPLACE VIEW meteostat_stations AS SELECT * FROM parquet_scan('%s');", stations_path))

# airports IATA/ICAO/TZ (limit to airports present in this month)
airports_df <- read.csv(
  "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
  header = FALSE, stringsAsFactors = FALSE
)
# V5=IATA, V6=ICAO, V12=TZ. exclude tz == '\N'
airports_tz <- subset(
  airports_df,
  V5 != "\\N" & V6 != "\\N" & V12 != "\\N" & nzchar(V12)
)[, c(5,6,12)]
names(airports_tz) <- c("iata","icao","tz")

iata_in_month <- dbGetQuery(con, "SELECT DISTINCT Origin AS iata FROM flights")$iata
airports_tz <- subset(airports_tz, iata %in% iata_in_month)

DBI::dbWriteTable(con, "airports_tz_raw", airports_tz, overwrite = TRUE)

# keep only rows with non-null, non-empty tz 
dbExecute(con, "
  CREATE OR REPLACE TABLE airports_tz AS
  SELECT iata, icao, tz
  FROM airports_tz_raw
  WHERE tz IS NOT NULL AND tz <> '' AND tz <> '\\N'
")

# map airport -> station
dbExecute(con, "
  CREATE OR REPLACE TABLE airport_station_map AS
  SELECT a.iata, a.icao, a.tz, s.id AS station_id
  FROM airports_tz a
  JOIN meteostat_stations s
    ON lower(s.\"identifiers.icao\") = lower(a.icao)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.iata ORDER BY s.id) = 1
")

# build features
dbExecute(con, sprintf("
  CREATE OR REPLACE TABLE features_%d_%s AS
  WITH f AS (
    SELECT
      fl.Year, fl.Month, fl.DayofMonth,
      fl.IATA_CODE_Reporting_Airline AS carrier,
      fl.Origin AS origin,
      fl.Dest AS dest,
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
    JOIN airport_station_map map
      ON map.iata = fl.Origin
    WHERE fl.CRSDepTime IS NOT NULL
      AND (fl.Cancelled IS NULL OR fl.Cancelled = 0)
      AND map.tz IS NOT NULL AND map.tz <> '' AND map.tz <> '\\N'
  ),
  fx AS (
    SELECT
      *,
      date_trunc('hour', sched_dep_tstz) AS dep_hr_bin,
      date_part('hour', sched_dep_tstz) AS dep_hour_local,
      date_part('dow', sched_dep_tstz) AS dow
    FROM f
  ),
  wx AS (
    SELECT
      station_id,
      CAST(time AS TIMESTAMPTZ) AS wx_ts,
      dwpt, rhum, prcp, snow, wdir, wspd,
      wpgt, pres, tsun, temp, coco
    FROM weather
  )
  SELECT
    fx.carrier, fx.origin, fx.dest,
    fx.sched_dep_tstz,
    fx.dep_hour_local, fx.dow,
    wx.dwpt, wx.rhum, wx.prcp, wx.snow, wx.wdir, wx.wspd,
    wx.wpgt, wx.pres, wx.tsun, wx.temp, wx.coco, fx.dep_delay_min,
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
", YEAR, MM))

# sanity checks
print(dbGetQuery(con, sprintf("SELECT COUNT(*) AS n FROM features_%d_%s", YEAR, MM)))
print(dbGetQuery(con, sprintf("
  SELECT origin, COUNT(*) AS n_rows,
         AVG(dep_delay_min) AS avg_delay,
         AVG(temp) AS avg_temp, AVG(prcp) AS avg_prcp
  FROM features_%d_%s
  GROUP BY 1 ORDER BY 2 DESC LIMIT 10
", YEAR, MM)))
print(dbGetQuery(con, sprintf("
  SELECT sched_dep_tstz, origin, carrier, dep_delay_min, temp, prcp, wspd
  FROM features_%d_%s
  ORDER BY sched_dep_tstz LIMIT 5
", YEAR, MM)))

# persist to parquet
out_parquet <- file.path("data/features", paste0("features_", YEAR, "_", MM, ".parquet"))
dir.create(dirname(out_parquet), recursive = TRUE, showWarnings = FALSE)
dbExecute(con, sprintf("
  COPY (SELECT * FROM features_%d_%s)
  TO '%s' (FORMAT PARQUET);
", YEAR, MM, out_parquet))
message("Wrote feature parquet: ", out_parquet)

dbExecute(con, "CHECKPOINT;")
dbDisconnect(con, shutdown = TRUE)
message("All done!")
