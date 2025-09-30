suppressPackageStartupMessages({
      library(DBI)
      library(duckdb)
      library(dplyr)
      library(tidymodels)
})

# train months: data.frame with cols: year, month (ints); test_month: single-row df year, month
train_baseline <- function(train_months, test_month){
      stopifnot(all(c('year','month') %in% names(train_months)),
                all(c('year','month') %in% names(test_month)),
                nrow(test_month)==1)
      train_paths <- mapply(features_parquet_path,
                            year = train_months$year,
                            month = train_months$month,
                            SIMPLIFY = TRUE, USE.NAMES = FALSE)
      test_path <- features_parquet_path(test_month$year[1], test_month$month[1])
      
      missing_train <- train_paths[!file.exists(train_paths)]
      if (length(missing_train)) stop('Missing train feature files:\n', paste(missing_train, collapse = '\n'))
      if (!file.exists(test_path)) stop('Missing test feature file: ', test_path)
      
      con <- open_duckdb(read_only = TRUE)
      on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
      ensure_icu(con)
      
      # 30 day rolling baseline with backfills (train only)
      quote_vec <- function(x) paste0("'", gsub("'", "''", x), "'")
      train_list_sql <- paste0('[', paste(quote_vec(train_paths), collapse = ","), "]")
      
      baseline_sql <- sprintf(
            "
            WITH train as (
                  SELECT * FROM parquet_scan(%s)
            ),
            test AS (
                  SELECT * FROM parquet_scan('%s')
            ),
            all_data AS (
                  SELECT 'train' AS split, * FROM train
                  UNION ALL
                  SELECT 'test' AS split, * FROM test
            ),
            -- rolling window (30 days)
            with_roll AS(
                  SELECT
                        *,
                        AVG(dep_delay_min) OVER (
                              PARTITION BY carrier, origin, dest
                              ORDER BY sched_dep_tstz
                              RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND INTERVAL 1 SECOND PRECEDING
                        ) AS yhat30
                  FROM all_data
            ),
            -- backfills, TRAIN only
            route_mean AS ( SELECT carrier, origin, dest, AVG(dep_delay_min) AS mean_route FROM train GROUP BY 1,2,3 ),
            carr_mean AS ( SELECT carrier, AVG(dep_delay_min) AS mean_carr FROM train GROUP BY 1 ),
            glob_mean AS ( SELECT AVG(dep_delay_min) AS mean_glob FROM train )
            SELECT
                  d.carrier, d.origin, d.dest, d.sched_dep_tstz, d.dep_delay_min,
                  COALESCE(d.yhat30, r.mean_route, c.mean_carr, g.mean_glob) AS yhat_baseline
            FROM with_roll d
            LEFT JOIN route_mean r USING (carrier, origin, dest)
            LEFT JOIN carr_mean c USING (carrier)
            CROSS JOIN glob_mean g
            WHERE d.split = 'test'
      ", train_list_sql, test_path)
      
      base_pred <- DBI::dbGetQuery(con, baseline_sql)
      mae_baseline <- with(base_pred, mean(abs(dep_delay_min - yhat_baseline), na.rm = TRUE))
      
      train_df <- DBI::dbGetQuery(con, sprintf('SELECT * FROM parquet_scan(%s)', train_list_sql))
      test_df <- DBI::dbGetQuery(con, sprintf("SELECT * FROM parquet_scan('%s')", test_path))
      
      predictors <- c("carrier","origin","dest","dep_hour_local","dow","temp","prcp","wspd","rhum","coco")
      reg_formula <- as.formula(paste('dep_delay_min ~', paste(predictors, collapse = ' + ')))
      
      rec <- recipe(reg_formula, data = train_df) |>
            step_novel(all_nominal_predictors()) |>
            step_unknown(all_nominal_predictors()) |>
            step_dummy(all_nominal_predictors(), one_hot=TRUE) |>
            step_ns(dep_hour_local, deg_free = 5) |>
            step_impute_median(all_numeric_predictors()) |>
            step_zv(all_predictors())
      
      mod <- linear_reg(penalty = 0.01, mixture = 0.5) |> set_engine('glmnet')
      
      wf <- workflow() |> add_model(mod) |> add_recipe(rec)
      fit_reg <- fit(wf, train_df)
      
      pred_reg <- predict(fit_reg, new_data=test_df) |> bind_cols(truth=test_df$dep_delay_min)
      
      metrics_reg <- metric_set(mae, rmse)
      reg_scores <- metrics_reg(pred_reg, truth=truth, estimate=.pred)
      
      dir.create('models', recursive = TRUE, showWarnings = FALSE)
      model_path <- file.path('models','baseline_glmnet_delay.rds')
      saveRDS(fit_reg, model_path)
      
      list(
            files = list(train=train_paths, test=test_path, model=model_path),
            metrics = list(
                  baseline30d_mae = mae_baseline,
                  glmnet_mae = reg_scores %>% filter(.metric == 'mae') %>% pull(.estimate),
                  glmnet_rmse = reg_scores %>% filter(.metric == 'rmse') %>% pull(.estimate)
            )
      )
      
      
}