// Dependencies
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

// Load `clean_cook_couty_data`
val data = spark.table("clean_cook_county_data")
val filteredData = data.filter(col("commitment_term") <= 100)

// Prep features and labels
val assembler = new VectorAssembler()
    .setInputCols(Array("charge_count", "age_at_incident", "bond_amount_current", "sentence_judge_onehot", "unit_onehot", "gender_onehot", "race_onehot", "court_name_onehot", "offense_category_onehot", "disposition_charged_offense_title_onehot"))
    .setOutputCol("features")
  
// Create a linear regression model
val lr = new LinearRegression()
    .setMaxIter(1000)
    .setRegParam(0.1)
    .setFeaturesCol("features")
    .setLabelCol("commitment_term")
    .setPredictionCol("prediction")

// Create pipeline and fit model
val pipeline = new Pipeline().setStages(Array(assembler, lr))
val model = pipeline.fit(filteredData)

// Make predictions
val predictions = model.transform(filteredData)

// Show predictions
predictions.select("commitment_term", "prediction").show()