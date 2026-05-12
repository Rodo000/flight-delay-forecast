# flight-delay-forecast
Flight delay forecasting model using U.S. flight data and Meteostat historical weather data. 
Our goal is to predict flight delay in minutes using flight schedule, carrier, airport and weather features.

# sources
-Flight data: Bureau of Transportation Statistics (BTS)
-Weather data: Meteostat weather data, hourly 

baseline model window timeframe is January 2022 - March 2023

# project status 

current pipeline stages:
1. BTS flight data ingestion
2. Meteostat weather ingestion
3. flight-weather feature building
4. Exploratory data analysis (EDA)
5. Baseline model

Current baseline models:
- 90-day historical rolling average
- ridge-regularized glmnet regression



