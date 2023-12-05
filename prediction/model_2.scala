// Stage 2 Model: Predict whether an individual is at risk of receiving an especially lengthy sentence

// Dependencies
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression

// Load `clean_cook_couty_data`
val data = spark.table("clean_cook_county_data")

// Apply model 1 to add a column
val model1Path = "hdfs:///user/hadoop/mnicolas/models/model_1"
val loadedModel1 = PipelineModel.load(model1Path)
val initialPredictions = loadedModel1.transform(data)
val resultDF = initialPredictions.withColumn("indicator_column", when(col("expected_value") > col("nth_percentile"), 1).otherwise(0))

// Prep features and labels
val assembler = new VectorAssembler()
    .setInputCols(Array("charge_count", "age_at_incident", "bond_amount_current", "sentence_judge_onehot", "unit_onehot", "gender_onehot", "race_onehot", "court_name_onehot", "offense_category_onehot", "disposition_charged_offense_title_onehot"))
    .setOutputCol("features_two")
  
// Create a linear regression model
val lr = new LogisticRegression()
    .setMaxIter(10000)
    .setRegParam(0.001)
    .setFeaturesCol("features_two")
    .setLabelCol("indicator_column")
    .setPredictionCol("predicted_risk")

// Create pipeline, fit model, generate predictions
val pipeline = new Pipeline().setStages(Array(assembler, lr))
val model = pipeline.fit(resultDF)

// Save model
val modelPath = "hdfs:///user/hadoop/mnicolas/models/model_2"
model.write.overwrite().save(modelPath)

// Script to load model
// val loadedModel = PipelineModel.load(modelPath)