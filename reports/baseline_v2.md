# Baseline models v2 documentation

A second version of baseline models was done as I returned to the project after a couple of months. This file documents some of the decisions.

### Modeling setup
- Data window: a period of 15 months (Jan 2022 - March 2023) was chosen to capture seasonal variations plus some extra months of data for validation and testing.
- Split: data for 2022 were chosen as training set. data corresponding to January 2023 was chosen as validation set, while February and March 2023 were saved for testing.
- A 90-day rolling average (with cutoff date) and a ridge-regularized glmnet model were chosen as baselines.

### Features 
- The following features were kept for prediction/training as they presented solid coverage: scheduled departure timestamp with timezone (`sched_dep_tstz`), airline (`carrier`), airport (`origin`), destination (`dest`), departure delay in minutes (`dep_delay_min`), departure hour in local time (`dep_hour_local`), day of week (`dow`), temperature in Celsius (`temp`), wind speed (`wspd`), relative humidity (`rhum`), wind direction (`wdir`), atmospheric pressure (`pres`), precipitation (`prcp`), weather condition (`coco`).
- The following features were dropped from modeling as they presented minimal or non-existing coverage: dew point (`dwpt`), snowfall (`snow`), wind gusts (`wpgt`) and sunshine duration (`tsun`).

### Results
The following are the baseline model metrics on validation and test sets:

  model           split            n   mae  rmse median_ae p90_ae   bias
  <chr>           <chr>        <int> <dbl> <dbl>     <dbl>  <dbl>  <dbl>
1 rolling_avg_90d validation  527499  26.4  61.4      12.7   57.1  0.462
2 rolling_avg_90d test       1062551  25.3  59.8      13     53.4  2.16 
3 glmnet_ridge    validation  527499  23.8  55.0      14.9   37.4 -0.481
4 glmnet_ridge    test       1062551  22.4  53.0      14.9   34.1  1.44 

### Interpretation
- The rolling average model is off by 25.3 minutes on average when predicting departure delays on the test set. It overpredicts delay by 2.16 minutes on average. It's slightly more sensitive to large delay errors with a rmse of 59.8 minutes, but has a solid median absolute error at 13. However, 90% of the test predictions are within 53.4 minutes.
- The glmnet model performs better across both validation and test sets, with an average test error of 22.4, almost 3 minutes lower than the rolling average model. It's rmse is also better at 53, almost 7 minutes lower than the rolling average model. However, it has a median absolute error of 14.9, almost 2 minutes more than the rolling average model. Still, 90% of the test prediction of this model are within 34.1 minutes of the actual value, almost 20 minutes lower than the rolling average model, a significant improvement. It's bias is also smaller, over predicting only by 1.44 minutes on average. 
- Overall, the glmnet model seems to be substantially better. 
