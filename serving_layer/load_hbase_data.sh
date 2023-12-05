hive -e "INSERT OVERWRITE TABLE nmarlton_cook_county_data_v0
SELECT
  *
FROM cook_county_batch_layer;"