// run from spark-shell with :load /path/to/scala/file.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Spark Session 
val spark = SparkSession.builder.appName("CountyDataTransform").getOrCreate()

// Set the configuration
spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", "true")

// Create schema
val schema = StructType(
  List(
    StructField("case_id", StringType, nullable = false),
    StructField("case_participant_id", StringType, nullable = true),
    StructField("received_date", StringType, nullable = true),
    StructField("offense_category", StringType, nullable = true),
    StructField("primary_charge", BooleanType, nullable = true),
    StructField("charge_id", StringType, nullable = true),
    StructField("charge_version_id", StringType, nullable = true),
    StructField("charge_offense_title", StringType, nullable = true),
    StructField("charge_count", StringType, nullable = true),
    StructField("chapter", StringType, nullable = true),
    StructField("act", StringType, nullable = true),
    StructField("section", StringType, nullable = true),
    StructField("class", StringType, nullable = true),
    StructField("aoic", StringType, nullable = true),
    StructField("event", StringType, nullable = true),
    StructField("event_date", StringType, nullable = true),
    StructField("age_at_incident", StringType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("race", StringType, nullable = true),
    StructField("incident_begin_date", StringType, nullable = true),
    StructField("law_enforcement_agency", StringType, nullable = true),
    StructField("arrest_date", StringType, nullable = true),
    StructField("felony_review_date", StringType, nullable = true),
    StructField("felony_review_result", StringType, nullable = true),
    StructField("arraignment_date", StringType, nullable = true),
    StructField("updated_offense_category", StringType, nullable = true)
  )
)

// Create dataframe
val initDF = spark.read.schema(schema).option("multiline",true).json("mnicolas/initiation_*.json")

// Describe 
initDF.show()
val dataSize = initDF.count()
println(s"data size: $dataSize")