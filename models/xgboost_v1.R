suppressPackageStartupMessages({
      library(arrow)
      library(dplyr)
      library(lubridate)
      library(Matrix)
      library(xgboost)
      library(tibble)
      library(readr)
      library(fs)
})

set.seed(42)

# config
feature_dir <- "data/features"
out_dir <- "artifacts/xgboost_v1"
baseline_metrics_path <- "artifacts/baseline_v2/metrics_baseline_v2.csv"

dir_create(out_dir)

target <- "dep_delay_min"

unused_weather <- c("dwpt", "snow", "wpgt", "tsun")

raw_keep_cols <- c(
      "sched_dep_tstz", "carrier", "origin",
      "dest", "dep_delay_min", "dep_hour_local",
      "dow", "temp", "wspd", "rhum",
      "wdir", "pres", "prcp", "coco"
)

numeric_vars <- c(
      "temp", "wspd", "rhum", "pres",
      "prcp", "wdir_sin", "wdir_cos"
)

categorical_vars <- c(
      "carrier", "origin", "dest", "dep_hour_local",
      "dow", "month", "coco", "has_prcp"
)

save_predictions <- FALSE

# metrics
metrics <- function(truth, pred, model, split){
      if (length(truth) != length(pred)){
            stop(
                  "truth and pred have different lengths for ",
                  model, " / ", split,
                  ": truth = ", length(truth),
                  ", pred = ", length(pred)
            )
      }
      
      err <- pred - truth
      
      tibble(
            model = model,
            split = split,
            n = length(truth),
            mae = mean(abs(err), na.rm = TRUE),
            rmse = sqrt(mean(err^2, na.rm = TRUE)),
            median_ae = median(abs(err), na.rm = TRUE),
            p90_ae = quantile(abs(err), 0.9, na.rm=TRUE, names = FALSE),
            bias = mean(err, na.rm = TRUE)
      )
}

# load data
message("Loading feature data...")

flights <- open_dataset(feature_dir, format = "parquet") %>% 
      select(any_of(raw_keep_cols)) %>% 
      collect()

missing_cols <- setdiff(raw_keep_cols, names(flights))

if (length(missing_cols) > 0){
      stop(
            "The following required columns are missing from the feature files: ",
            paste(missing_cols, collapse = ", ")
      )
}

if (!inherits(flights$sched_dep_tstz, "POSIXt")){
      flights <- flights %>% 
            mutate(sched_dep_tstz = ymd_hms(
                  sched_dep_tstz,
                  quiet = TRUE,
                  tz = "UTC"
            )
      )
}

flights <- flights %>% 
      mutate(
            dep_date = as.Date(sched_dep_tstz),
            month = month(dep_date)
      ) %>% filter(
            dep_date >= as.Date("2022-01-01"),
            dep_date < as.Date("2023-04-01")
      ) %>% 
      filter(!is.na(.data[[target]])) %>% 
      select(-any_of(unused_weather))

# feature engineering

flights <- flights %>% 
      mutate(
            split = case_when(
                  dep_date >= as.Date("2022-01-01") & dep_date < as.Date("2023-01-01") ~ "train",
                  dep_date >= as.Date("2023-01-01") & dep_date < as.Date("2023-02-01") ~ "validation",
                  dep_date >= as.Date("2023-02-01") & dep_date < as.Date("2023-04-01") ~ "test",
                  TRUE ~ NA_character_
            ),
            wdir_sin = sin(wdir * pi / 180),
            wdir_cos = cos(wdir * pi / 180),
            has_prcp = case_when(
                  is.na(prcp) ~ "missing",
                  prcp > 0 ~ "yes",
                  TRUE~ "no"
            )
      ) %>% 
      filter(!is.na(split))

split_counts <- flights %>% 
      count(split)

print(split_counts)

train_idx <- flights$split == "train"
valid_idx <- flights$split == "validation"
test_idx <- flights$split == "test"

# impute numeric variables using training medians

message("Imputing numeric weather features...")

numeric_medians <- sapply(numeric_vars, function(v){
      med <- median(flights[[v]][train_idx], na.rm = TRUE)
      
      if (is.na(med) || is.nan(med)) {
            med <- 0
      }
      med
})

for (v in numeric_vars){
      flights[[v]][is.na(flights[[v]])] <- numeric_medians[[v]]
}

# handle categorical vairables

message("Encoding categorical variables...")

factor_levels <- list()

for (v in categorical_vars){
      train_values <- as.character(flights[[v]][train_idx])
      train_values[is.na(train_values) | train_values == ""] <- "missing"
      
      levels_v <- sort(unique(c(train_values, "missing", "other")))
      factor_levels[[v]] <- levels_v
      
      all_values <- as.character(flights[[v]])
      all_values[is.na(all_values) | all_values == ""] <- "missing"
      all_values[!(all_values %in% levels_v)] <- "other"
      
      flights[[v]] <- factor(all_values, levels = levels_v)
}

# build sparse model matrix

message("Building sparse model matrix...")

predictor_vars <- c(categorical_vars, numeric_vars)

model_df <- flights %>% 
      select(all_of(predictor_vars))

x_all <- sparse.model.matrix(
      ~ . -1,
      data = model_df
)

y_all <- flights[[target]]

x_train <- x_all[train_idx, ]
x_valid <- x_all[valid_idx, ] 
x_test <- x_all[test_idx, ]

y_train <- y_all[train_idx]
y_valid <- y_all[valid_idx]
y_test <- y_all[test_idx]

rm(model_df, x_all)
gc()

# D Matrix objects
message("Creating XGBoost Dmatrix objects...")

dtrain <- xgb.DMatrix(data = x_train, label = y_train, missing = NA)
dvalid <- xgb.DMatrix(data = x_valid, label = y_valid, missing = NA)
dtest <- xgb.DMatrix(data = x_test, label = y_test, missing = NA)

rm(x_train, x_valid, x_test)
gc()

# train XGBoost 
message("Training XGBoost v1...")

params <- list(
      booster = "gbtree",
      objective = "reg:squarederror",
      eval_metric = "mae",
      
      # conservative first pass for millions of rows
      max_depth = 6,
      eta = 0.05,
      min_child_weight = 100,
      subsample = 0.8,
      colsample_bytree = 0.8,
      
      # regularization
      lambda = 1,
      alpha = 0,
      
      # fast tree construction
      tree_method = "hist",
      
      # cpu 
      nthread = parallel::detectCores(logical = TRUE)
)

xgb_model <- xgb.train(
      params = params,
      data = dtrain, 
      nrounds = 1000,
      evals = list(
            train = dtrain,
            validation = dvalid
      ),
      early_stopping_rounds = 40,
      print_every_n = 25,
      verbose = 1
)

message("Best iteration: ", xgb_model$best_iteration)
message("Best validation MAE: ", xgb_model$best_score)

# predict
message("Generating predictions...")

pred_valid <- predict(xgb_model, dvalid)
pred_test <- predict(xgb_model, dtest)

# evaluate
xgb_metrics <- bind_rows(
      metrics(y_valid, pred_valid, "xgboost_v1", "validation"),
      metrics(y_test, pred_test, "xgboost_v1", "test")
)

print(xgb_metrics)

write_csv(
      xgb_metrics, 
      file.path(out_dir, "metrics_xgboost_v1.csv")
)

# compare vs baseline
if (file.exists(baseline_metrics_path)){
      baseline_metrics <- read_csv(
            baseline_metrics_path,
            show_col_types = FALSE
      )
      
      metrics_comparison <- bind_rows(
            baseline_metrics, xgb_metrics
      )
      
      print(metrics_comparison)
      
      write_csv(
            metrics_comparison,
            file.path(out_dir, "metrics_with_baselines_xgboost_v1.csv")
      )
} else{
      message("Baseline metrics file not found: ", baseline_metrics_path)
}

# save model + metadata
xgb.save(
      xgb_model, 
      file.path(out_dir, "xgboost_v1.ubj")
)

metadata <- list(
      target = target,
      unused_weather = unused_weather,
      predictor_vars = predictor_vars, 
      numeric_vars = numeric_vars,
      categorical_vars = categorical_vars,
      numeric_medians = numeric_medians,
      factor_levels = factor_levels,
      params = params,
      best_iteration = xgb_model$best_iteration,
      best_score = xgb_model$best_score
)

saveRDS(
      metadata,
      file.path(out_dir, "xgboost_v1_metadata.rds")
)

# save predictions (optional)
if (save_predictions){
      validation_predictions <- tibble(
            split = "validation",
            truth = y_valid,
            pred = pred_valid,
            err = pred_valid - y_valid
      )
      
      test_predictions <- tibble(
            split = "test",
            truth = y_test,
            pred  = pred_test,
            err = pred_test - y_test
      )
      
      write_parquet(
            validation_predictions,
            file.path(out_dir, "predictions_validation.parquet")
      )
      
      write_parquet(
            test_predictions, 
            file.path(out_dir, "predictions_test.parquet")
      )
}

message("Done. Outputs saved to: ", out_dir)

