after the cook county court data is transformed into the clean_cook_county_data Hive table, modify data and pull into the HBase serving layer

### Files:

-`create_hbase_table.hql`: 
    Create HBase table (`nmarlton_court_data`) that the app will read to retrieve court records.
-`load_hbase_data.hql`:
    Populate the `nmarlton_court_data` table
-`load_hbase_data.hql`:
    Update the `nmarlton_court_data` table with new batch layer data (TODO: implement this script)

### Data Glossary:

https://www.cookcountystatesattorney.org/resources/how-read-data