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
val speedDF = filterData.filter(col("incident_begin_date") > lit("2021-12-31")).select(
    "case_id", "updated_offense_category", "disposition_charged_offense_title", "incident_begin_date", "judge",
    "charge_count", "age_at_incident", "bond_amount_current", "sentence_judge_onehot", "unit_onehot", "gender_onehot", 
    "race_onehot", "court_name_onehot", "offense_category_onehot", "disposition_charged_offense_title_onehot",
    "nth_percentile", "commitment_term" // why do I need this for my model??
)

// speedDF.show()
// Generate 
// Save speedDF to Hive

// Generate Hive table to push to serving layer
val rawBatchDF = filterData.filter(col("incident_begin_date") <= lit("2021-12-31"))

// Apply model 1 to add a column
val model1Path = "hdfs:///user/hadoop/mnicolas/models/model_1"
val loadedModel1 = PipelineModel.load(model1Path)
val predictions = loadedModel1.transform(rawBatchDF)

// Apply model 2 to add a column
val model2Path = "hdfs:///user/hadoop/mnicolas/models/model_2"
val loadedModel2 = PipelineModel.load(model2Path)
val model2Predictions = loadedModel2.transform(predictions)

// Extract probability scores and show 
val probabilityUDF = udf((probability: org.apache.spark.ml.linalg.Vector) => probability(1).toDouble)
val finalPredictions = model2Predictions.withColumn("predicted_risk", probabilityUDF(col("probability")))

// Generate percentiles
val meanValue = 0.6306682392110071 // calculated from training data
val stddevValue = 0.07616869432724285 // calculated from training data
val percentileUDF = udf((value: Double) => (org.apache.commons.math3.special.Erf.erf((value - meanValue) / (stddevValue * math.sqrt(2.0))) + 1.0) / 2.0)
val finalPercentileDF = finalPredictions.withColumn("predicted_risk_percentile", percentileUDF(col("predicted_risk")))


// Generate third value
val batchDF = finalPercentileDF.select(
    col("case_id").alias("case_id"),
    col("updated_offense_category").alias("offense_category"),
    col("disposition_charged_offense_title").alias("offense_title"),
    substring(col("incident_begin_date"), 1, 10).alias("incident_date"),
    col("judge").alias("judge"),
    // col("expected_value").alias("expected_value"),
    // col("predicted_risk").alias("predicted_risk"),
    // col("predicted_risk_percentile").alias("predicted_risk_percentile"),
    round(col("predicted_risk_percentile") * 100, 0).alias("predicted_risk_percentile")
)

batchDF.show()

// Save batchDF to Hive