library(arrow)
library(dplyr)
library(lubridate)
library(glmnet)
library(Matrix)
library(readr)

set.seed(123)

# config
feature_dir <- "data/features"
out_dir <- "artifacts/baseline_v2"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

unused_weather <- c("dwpt", "snow", "wpgt", "tsun")

keep_cols <- c(
      "sched_dep_tstz", "carrier", "origin", "dest",
      "dep_delay_min",
      "dep_hour_local", "dow",
      "temp", "wspd", "rhum", "wdir", "pres", "prcp", "coco"
)

# load parquet files
flights <- open_dataset(feature_dir) %>% 
      select(any_of(keep_cols)) %>% 
      collect()

# parse sched_dep_tstz if collected as character
if (!inherits(flights$sched_dep_tstz, "POSIXt")) {
      flights <- flights %>% 
            mutate(sched_dep_tstz = ymd_hms(sched_dep_tstz, quiet=TRUE, tz="UTC"))
}

flights <- flights %>% 
      mutate(
            dep_date = as.Date(sched_dep_tstz),
            month = month(dep_date)
      ) %>%
      filter(dep_date >= as.Date("2022-01-01"),
             dep_date < as.Date("2023-04-01"),
             !is.na(dep_delay_min))

# if dep_hout_local or dow missing/broken, create them
if (!"dep_hour_local" %in% names(flights) || all(is.na(flights$dep_hour_local))){
      flights <- flights %>% 
            mutate(dep_hour_local = hour(sched_dep_tstz))
}

if (!"dow" %in% names(flights) || all(is.na(flights$dow))){
      flights <- flights %>% 
            mutate(dow = wday(dep_date, label = TRUE, abbr = TRUE, week_start = 1))
} 

# chronological split
train <- flights %>% 
      filter(dep_date >= as.Date("2022-01-01"), dep_date < as.Date("2023-01-01"))
valid <- flights %>% 
      filter(dep_date >= as.Date("2023-01-01"), dep_date < as.Date("2023-02-01"))
test <- flights %>% 
      filter(dep_date >= as.Date("2023-02-01"), dep_date < as.Date("2023-04-01"))

split_counts <- tibble(
      split = c("train","validation","test"),
      n = c(nrow(train), nrow(valid), nrow(test))
)

write_csv(split_counts, file.path(out_dir, "split_counts.csv"))
print(split_counts)

# metrics helper
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
            mae = mean(abs(err), na.rm=TRUE),
            rmse = sqrt(mean(err^2, na.rm=TRUE)),
            median_ae = median(abs(err), na.rm = TRUE),
            p90_ae = quantile(abs(err), 0.90, na.rm=TRUE, names=FALSE),
            bias = mean(err, na.rm=TRUE)
      )
}

# baseline 1 - 90-day historical average
predict_rolling_avg <- function(history, new_data, cutoff_date, window_days = 90){
   hist <- history %>% 
      filter(dep_date >= cutoff_date - days(window_days), dep_date < cutoff_date)
   
   global_mean <- mean(hist$dep_delay_min, na.rm = TRUE)
   
   # main grouoed average (carrier + route + hour + day_of_week)
   avg_main <- hist %>% 
      group_by(carrier, origin, dest, dep_hour_local, dow) %>% 
      summarise(pred_main = mean(dep_delay_min, na.rm = TRUE), .groups = "drop")
   
   # fallback grouped average (route + hour)
   avg_fallback <- hist %>% 
      group_by(origin, dest, dep_hour_local) %>% 
      summarise(pred_fallback = mean(dep_delay_min, na.rm = TRUE), .groups = "drop")
   
   new_data %>% 
      left_join(avg_main, by = c("carrier", "origin", "dest", "dep_hour_local", "dow")) %>% 
      left_join(avg_fallback, by = c("origin", "dest", "dep_hour_local")) %>% 
      mutate(pred_rolling_avg = coalesce(pred_main, pred_fallback, global_mean)) %>% 
      select(-pred_main, -pred_fallback)
}

valid_roll <- predict_rolling_avg(
   history = train,
   new_data = valid,
   cutoff_date = as.Date("2023-01-01")
)

test_roll <- predict_rolling_avg(
   history = bind_rows(train, valid),
   new_data = test,
   cutoff_date = as.Date("2023-02-01")
)

rolling_metrics <- bind_rows(
   metrics(valid_roll$dep_delay_min, valid_roll$pred_rolling_avg, "rolling_avg_90d", "validation"),
   metrics(test_roll$dep_delay_min, test_roll$pred_rolling_avg, "rolling_avg_90d", "test")
)

# bawseline 2: glmnet

prep_glmnet <- function(df){
   df %>% 
      mutate(
         carrier = factor(carrier),
         origin = factor(origin),
         dest = factor(dest),
         dow = factor(dow),
         dep_hour_local = factor(dep_hour_local),
         month = factor(month),
         coco = factor(ifelse(is.na(coco), "missing", as.character(coco))),
         has_prcp = ifelse(is.na(prcp), NA_real_, as.numeric(prcp > 0)),
         wdir_sin = sin(2 * pi * wdir/360),
         wdir_cos = cos(2 * pi * wdir/360)
      ) %>% 
      select(
         dep_delay_min,
         carrier, origin, dest,
         dep_hour_local, dow, month,
         temp, wspd, rhum, pres, prcp, has_prcp,
         wdir_sin, wdir_cos,
         coco
      )
}

train_glm <- prep_glmnet(train)
valid_glm <- prep_glmnet(valid)
test_glm <- prep_glmnet(test)

# align factor levels
factor_cols <- c("carrier", "origin", "dest", "dep_hour_local", "dow", "month", "coco")

for (col in factor_cols){
   levels_train <- sort(unique(as.character(train_glm[[col]])))
   levels_train <- levels_train[!is.na(levels_train)]
   levels_model <- c(levels_train, "other")
   
   train_vals <- as.character(train_glm[[col]])
   valid_vals <- as.character(valid_glm[[col]])
   test_vals <- as.character(test_glm[[col]])
   
   train_vals[is.na(train_vals)] <- "other"
   valid_vals[is.na(valid_vals) | !valid_vals %in% levels_train] <- "other"
   test_vals[is.na(test_vals) | !test_vals %in% levels_train] <- "other"
   
   train_glm[[col]] <- factor(train_vals, levels = levels_model)
   valid_glm[[col]] <- factor(valid_vals, levels = levels_model)
   test_glm[[col]] <- factor(test_vals, levels = levels_model)
}

# median imputation on numeric cols
num_cols <- names(train_glm)[sapply(train_glm, is.numeric)]
num_cols <- setdiff(num_cols, "dep_delay_min")

for (col in num_cols){
   med <- median(train_glm[[col]], na.rm = TRUE)
   if (!is.finite(med)) med <- 0
   
   train_glm[[col]][is.na(train_glm[[col]])] <- med
   valid_glm[[col]][is.na(valid_glm[[col]])] <- med
   test_glm[[col]][is.na(test_glm[[col]])] <- med
}

form <- dep_delay_min ~ .

x_train <- sparse.model.matrix(form, train_glm)[, -1]
y_train <- train_glm$dep_delay_min

x_valid <- sparse.model.matrix(form, valid_glm)[, -1]
y_valid <- valid_glm$dep_delay_min

x_test <- sparse.model.matrix(form, test_glm)[, -1]
y_test <- test_glm$dep_delay_min

stopifnot(nrow(x_train) == length(y_train))
stopifnot(nrow(x_valid) == length(y_valid))
stopifnot(nrow(x_test) == length(y_test))

# fit glmnet on train, choose lambda with lowest MAE
glmnet_fit <- glmnet(
   x = x_train,
   y = y_train,
   family = "gaussian",
   alpha = 0, 
   standardize = TRUE
)

valid_pred_all <- predict(glmnet_fit, newx = x_valid, s = glmnet_fit$lambda)
valid_mae_by_lambda <- colMeans(abs(valid_pred_all - y_valid))
best_lambda <- glmnet_fit$lambda[which.min(valid_mae_by_lambda)]

valid_pred_glmnet <- as.numeric(predict(glmnet_fit, newx = x_valid, s=best_lambda))

# refit glmnet with best lambda on fit + validation, evaluate on test
train_valid_glm <- bind_rows(train_glm, valid_glm)
x_train_valid <- sparse.model.matrix(form, train_valid_glm)[, -1]
y_train_valid <- train_valid_glm$dep_delay_min

final_glmnet <- glmnet(
   x = x_train_valid,
   y = y_train_valid,
   family = "gaussian",
   alpha = 0,
   lambda = best_lambda,
   standardize = TRUE
)

test_pred_glmnet <- as.numeric(predict(final_glmnet, newx=x_test, s = best_lambda))

glmnet_metrics <- bind_rows(
   metrics(y_valid, valid_pred_glmnet, "glmnet_ridge", "validation"),
   metrics(y_test, test_pred_glmnet, "glmnet_ridge", "test")
)

# save results
all_metrics <- bind_rows(rolling_metrics, glmnet_metrics)
write_csv(all_metrics, file.path(out_dir, "metrics_baseline_v2.csv"))
print(all_metrics)

preds_valid <- valid_roll %>%
   transmute(
      split = "validation",
      sched_dep_tstz,
      carrier, origin, dest,
      dep_delay_min,
      pred_rolling_avg,
      pred_glmnet = valid_pred_glmnet
   )

preds_test <- test_roll %>%
   transmute(
      split = "test",
      sched_dep_tstz,
      carrier, origin, dest,
      dep_delay_min,
      pred_rolling_avg,
      pred_glmnet = test_pred_glmnet
   )

write_parquet(preds_valid, file.path(out_dir, "predictions_validation.parquet"))
write_parquet(preds_test, file.path(out_dir, "predictions_test.parquet"))

saveRDS(
   list(
      model = final_glmnet,
      best_lambda = best_lambda,
      formula = form,
      dropped_columns = unused_weather
   ),
   file.path(out_dir, "glmnet_baseline_v2.rds")
)

message("Done. Metrics saved to: ", file.path(out_dir, "metrics_baseline_v2.csv"))



























