hive -e "
CREATE TABLE nmarlton_cook_county_data_v0 (
    case_id STRING,
    offense_category STRING,
    offense_title STRING,
    received_date STRING, 
    judge STRING, 
    predicted_risk_percentile STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    \"hbase.columns.mapping\" = \":key,family:offense_category,family:offense_title,family:received_date,family:judge,family:predicted_risk_percentile\"
)
TBLPROPERTIES (\"hbase.table.name\" = \"nmarlton_cook_county_data_v0\");
"