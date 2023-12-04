-- Hive query to write into Hbase
INSERT OVERWRITE TABLE nmarlton_hw52_annual_airport_weather_delays
SELECT
  airport_year,
  clear_flights,
  clear_delays,
  fog_flights,
  fog_delays,
  rain_flights,
  rain_delays,
  snow_flights,
  snow_delays,
  hail_flights,
  hail_delays,
  thunder_flights,
  thunder_delays,
  tornado_flights,
  tornado_delays
FROM nmarlton_hw52_annual_airport_weather_delays;