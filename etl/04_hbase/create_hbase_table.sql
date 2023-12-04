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

