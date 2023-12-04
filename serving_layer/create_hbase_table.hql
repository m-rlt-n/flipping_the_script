CREATE TABLE hbase_table (
    case_id string,
    updated_offense_category string,
    disposition_charged_offense_title string,
    received_date string, 
    judge string, 
    court_name string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,family:column1,family:column2"
)
TBLPROPERTIES ("hbase.table.name" = "your_hbase_table");


5.1: the entire solution to homework 5.1, including the pros and cons are captured in the homework_51_wordcount.txt file. 

5.2: 
- Spark script to populate the table (hive style)
val sqlScript = """CREATE TABLE default.nmarlton_hw52_annual_airport_weather_delays
        SELECT
        CONCAT(dest_name, d.year) as airport_year
        , SUM(CASE WHEN fog = false AND rain = false AND snow = false
                AND hail = false AND thunder = false AND tornado = false
                THEN 1 ELSE 0 END) clear_flights
        , SUM(CASE WHEN fog = false AND rain = false AND snow = false
                AND hail = false AND thunder = false AND tornado = false
                AND d.dep_delay >= 0
                THEN d.dep_delay ELSE 0 END) clear_delays
        , SUM(CASE WHEN fog = true THEN 1 ELSE 0 END) fog_flights
        , SUM(CASE WHEN fog = true AND d.dep_delay >= 0
                THEN d.dep_delay ELSE 0 END) fog_delays
        , SUM(CASE WHEN rain = true THEN 1 ELSE 0 END) rain_flights
        , SUM(CASE WHEN rain = true AND d.dep_delay >= 0
                THEN dep_delay ELSE 0 END) rain_delays
        , SUM(CASE WHEN snow = true THEN 1 ELSE 0 END) snow_flights
        , SUM(CASE WHEN snow = true AND d.dep_delay >= 0
                THEN dep_delay ELSE 0 END) snow_delays
        , SUM(CASE WHEN hail = true THEN 1 ELSE 0 END) hail_flights
        , SUM(CASE WHEN hail = true AND d.dep_delay >= 0
                THEN dep_delay ELSE 0 END) hail_delays
        , SUM(CASE WHEN thunder = true THEN 1 ELSE 0 END) thunder_flights
        , SUM(CASE WHEN thunder = true AND d.dep_delay >= 0
                THEN dep_delay ELSE 0 END) thunder_delays
        , SUM(CASE WHEN tornado = true THEN 1 ELSE 0 END) tornado_flights
        , SUM(CASE WHEN tornado = true AND d.dep_delay >= 0
                THEN dep_delay ELSE 0 END) tornado_delays
FROM delays d
        LEFT JOIN weathersummary w
                ON w.station = d.origin_code
                AND w.year = d.year
                AND w.month = d.month
                AND w.day = d.day
WHERE origin_name = 'ORD'
GROUP BY CONCAT(dest_name, d.year)"""

spark.sql(sqlScript)

- Spark script to populate the table (object.method style) 
TBD

- Hive query to define the external HBase table
CREATE TABLE nmarlton_hw52_annual_airport_weather_delays (
  airport_year STRING,
  clear_flights BIGINT,
  clear_delays BIGINT,
  fog_flights BIGINT,
  fog_delays BIGINT,
  rain_flights BIGINT,
  rain_delays BIGINT,
  snow_flights BIGINT,
  snow_delays BIGINT,
  hail_flights BIGINT,
  hail_delays BIGINT,
  thunder_flights BIGINT,
  thunder_delays BIGINT,
  tornado_flights BIGINT,
  tornado_delays BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,delay:clear_flights,delay:clear_delays,delay:fog_flights,delay:fog_delays,delay:rain_flights,delay:rain_delays,delay:snow_flights,delay:snow_delays,delay:hail_flights,delay:hail_delays,delay:thunder_flights,delay:thunder_delays,delay:tornado_flights,delay:tornado_delays'
)
TBLPROPERTIES (
  'hbase.table.name' = 'nmarlton_hw52_annual_airport_weather_delays'
);

- Hive query to write into Hbase
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

- Minor updates to app.js 
change table reference
