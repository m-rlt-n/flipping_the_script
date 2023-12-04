// Stage 1 Model: Predict sentence length and flag “especially lengthy” sentences
// 'because data that mark sentences as having been especially lengthy do not exist, we begin the process of 
// build-ing our risk assessment model by inferring which sentences were "especially lengthy."' (Meyer, et al.)
// The paper implements their model using Heteroscedastic Bayesian autoregressive trees (HBART)
// This project approximates the "especially lengthy sentence" flag by comparing a prediction to the 75th percentile for a given
// combination of "charge_count", "offense_category_onehot", "disposition_charged_offense_title_onehot"

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

// // Create a GBTRegressor model
// val gbt = new GBTRegressor()
//     .setMaxIter(100)
//     .setFeaturesCol("features")
//     .setLabelCol("commitment_term")
//     .setPredictionCol("expected_value")
//     // .setPredictionCol("stddev")  // This sets the column name for standard deviations

// // Create pipeline and fit model
// val pipeline = new Pipeline().setStages(Array(assembler, gbt))
// val model = pipeline.fit(filteredData)

// // Assuming you have a DataFrame `predictions` with a column named `prediction`
// // val residuals = predictions.withColumn("residual", col("commitment_term") - col("expected_value"))

// // Calculate standard error of residuals
// // val residualStdDev = math.sqrt(residuals.agg(expr("VAR_SAMP(residual)")).collect()(0)(0).asInstanceOf[Double])

// // Set the confidence level (e.g., 95%)
// val confidenceLevel = 0.95

// // Get the z-value for the confidence level
// val zValue = org.apache.commons.math3.special.Erf.erfInv(confidenceLevel)

// // Calculate the margin of error (z-value * standard error of residuals)
// val marginOfError = zValue * residualStdDev

// // Calculate lower and upper bounds of the prediction interval
// val lowerBound = col("prediction") - marginOfError
// val upperBound = col("prediction") + marginOfError

// // Add lower and upper bounds to the predictions DataFrame
// val predictionInterval = predictions.withColumn("lower_bound", lowerBound).withColumn("upper_bound", upperBound)

// // Show the result
// predictionInterval.select("label", "prediction", "lower_bound", "upper_bound").show()



// // // Calculate upper bound of the predictive interval
// // val alpha = 0.1
// // val upperBoundUDF = udf((expectedValue: Double, conditionalVariance: Double) =>
// //   expectedValue + math.sqrt(conditionalVariance) * org.apache.commons.math3.special.Erf.erfInv(2 * alpha - 1)
// // )

// // val predictionsWithUpperBound = predictions
// //   .withColumn("upper_bound", upperBoundUDF(col("expected_value"), col("conditional_variance")))

// // // Define especially lengthy sentences based on the threshold
// // val especiallyLengthyUDF = udf((sentenceLength: Double, upperBound: Double) => if (sentenceLength > upperBound) 1 else 0)

// // val predictionsWithIndicator = predictionsWithUpperBound
// //   .withColumn("especially_lengthy", especiallyLengthyUDF(col("actual_sentence_length"), col("upper_bound")))

// // // Show the results
// // predictionsWithIndicator.select(/* your relevant columns */).show()







// Make predictions
val predictions = model.transform(filteredData)

// Show predictions
predictions.select("commitment_term", "expected_value", "nth_percentile").show()
//
// Specify model path and save model
val modelPath = "hdfs:///user/hadoop/mnicolas/models/model_1"
model.write.overwrite().save(modelPath)

// Script to load model
// val loadedModel = PipelineModel.load(modelPath)