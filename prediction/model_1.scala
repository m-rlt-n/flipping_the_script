// Stage 1 Model: Predict sentence length and flag “especially lengthy” sentences
// 'because data that mark sentences as having been especially lengthy do not exist, we begin the process of 
// build-ing our risk assessment model by inferring which sentences were "especially lengthy."' (Meyer, et al.)
// The paper implements their model using Heteroscedastic Bayesian Autoregressive Trees (HBART)
// This project approximates the "especially lengthy sentence" flag by comparing a prediction to the 75th percentile of 
// sentence length for a given combination of "charge_count", "offense_category_onehot", "disposition_charged_offense_title_onehot"

// Dependencies
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.PipelineModel

// Load `clean_cook_couty_data`
val data = spark.table("clean_cook_county_data")
val filteredData = data.filter(col("commitment_term") <= 100)
// val stdDevValue = filteredData.select(stddev("commitment_term").alias("stddev")).collect()(0).getAs[Double]("stddev")

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
    .setPredictionCol("expected_value")

// Create pipeline and fit model
val pipeline = new Pipeline().setStages(Array(assembler, lr))
val model = pipeline.fit(filteredData)

// // Show predictions
// val predictions = model.transform(filteredData)
// predictions.select("commitment_term", "expected_value", "nth_percentile").show()

// Specify model path and save model
val modelPath = "hdfs:///user/hadoop/mnicolas/models/model_1"
model.write.overwrite().save(modelPath)

// Script to load model
// val loadedModel = PipelineModel.load(modelPath)