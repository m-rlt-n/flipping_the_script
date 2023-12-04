// Read only the columns we want from the clean_cook_county_data Hive table
// Create two tables:
    // (1) cook_county_batch_layer - everthing excl. data after 2021-12-31 (which will be used for simulating a speed layer)
    // (2) cook_county_speed_layer - data produced after 2021-12-31 (to be used for simulating a speed layer)
// Batch Layer:
    // Run both models and add three columns:
        // Mean sentence durration
        // Predicted sentence durration
        // Risk score (methodology unknown)
    // App will query the details of the case and the model results
    // These fields can be written directly in the serving layer

// Speed Layer: 
    // Stream in data from the cook_county_speed_layer tables
    // As it streams in, run both models, and append the three new values (Mean durration, Predicited durration, risk score) 
    // Save these values to HBase

val data = spark.table("clean_cook_county_data")
val filterData = data.filter(col("commitment_term") <= 100)
// run first model
// run second model
// val columnDropDF = // Load `clean_cook_couty_data`

// //
// case_id string,
//     updated_offense_category string,
//     disposition_charged_offense_title string,
//     received_date string, 
//     judge string, 
//     court_name string
// )

// Set time parser policy and generate speedDF for streaming
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
val speedDF = filterData.filter(col("incident_begin_date") < lit("2021-12-31")).select(
    "case_id", "updated_offense_category", "disposition_charged_offense_title", "incident_begin_date", "judge",
    "charge_count", "age_at_incident", "bond_amount_current", "sentence_judge_onehot", "unit_onehot", "gender_onehot", 
    "race_onehot", "court_name_onehot", "offense_category_onehot", "disposition_charged_offense_title_onehot",
    "commitment_term" // why do I need this for my model??
)
val speedDF = filterData.filter(col("incident_begin_date") > lit("2021-12-31")).select(
  col("case_id").alias("case_id"),
  col("updated_offense_category").alias("offense_category"),
  col("disposition_charged_offense_title").alias("offense_title"),
  col("incident_begin_date").alias("incident_date"),
  col("judge").alias("judge"),
)

speedDF.show()
// Generate 
