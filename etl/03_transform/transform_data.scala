import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Spark Session
val spark = SparkSession.builder.appName("CountyDataTransform").getOrCreate()

// Set the configuration
spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", "true")

// Schema for Initation data
val init_schema = StructType(
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

// Load data into initDF
val initDF = spark.read.schema(init_schema).option("multiline",true).json("mnicolas/initiation_*.json")
initDF.show()
val initSize = initDF.count()
println(s"data size: $initSize")

// Schema for Disposition data
val dispo_schema = StructType(
  List(
    StructField("case_id", StringType, nullable = true),
    StructField("case_participant_id", StringType, nullable = true),
    StructField("offense_category", StringType, nullable = true),
    StructField("primary_charge", BooleanType, nullable = true),
    StructField("charge_id", StringType, nullable = true),
    StructField("charge_version_id", StringType, nullable = true),
    StructField("charge_offense_title", StringType, nullable = true),
    StructField("judge", StringType, nullable = true),
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
    StructField("incident_end_date", StringType, nullable = true),
    StructField("arrest_date", StringType, nullable = true),
    StructField("law_enforcement_agency", StringType, nullable = true),
    StructField("unit", StringType, nullable = true),
    StructField("incident_city", StringType, nullable = true),
    StructField("received_date", StringType, nullable = true),
    StructField("arraignment_date", StringType, nullable = true),
    StructField("updated_offense_category", StringType, nullable = true),
    StructField("charge_count", IntegerType, nullable = true)
  )
)

// Load disposition data into dispoDF
val dispoDF = spark.read.schema(init_schema).option("multiline",true).json("mnicolas/disposition_*.json")
dispoDF.show()
val dispoSize = dispoDF.count()
println(s"data size: $dispoSize")

// Schema for Sentencing data
val sent_schema = StructType(
  List(
    StructField("case_id", StringType, nullable = true),
    StructField("case_participant_id", StringType, nullable = true),
    StructField("offense_category", StringType, nullable = true),
    StructField("primary_charge", BooleanType, nullable = true),
    StructField("charge_id", StringType, nullable = true),
    StructField("charge_version_id", StringType, nullable = true),
    StructField("disposition_charged_offense_title", StringType, nullable = true),
    StructField("charge_count", StringType, nullable = true),
    StructField("disposition_date", StringType, nullable = true),
    StructField("disposition_charged_chapter", StringType, nullable = true),
    StructField("disposition_charged_act", StringType, nullable = true),
    StructField("disposition_charged_section", StringType, nullable = true),
    StructField("disposition_charged_class", StringType, nullable = true),
    StructField("disposition_charged_aoic", StringType, nullable = true),
    StructField("charge_disposition", StringType, nullable = true),
    StructField("sentence_judge", StringType, nullable = true),
    StructField("court_name", StringType, nullable = true),
    StructField("court_facility", StringType, nullable = true),
    StructField("sentence_phase", StringType, nullable = true),
    StructField("sentence_date", StringType, nullable = true),
    StructField("sentence_type", StringType, nullable = true),
    StructField("current_sentence", BooleanType, nullable = true),
    StructField("commitment_type", StringType, nullable = true),
    StructField("commitment_term", StringType, nullable = true),
    StructField("commitment_unit", StringType, nullable = true),
    StructField("length_of_case_in_days", StringType, nullable = true),
    StructField("age_at_incident", StringType, nullable = true),
    StructField("race", StringType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("incident_city", StringType, nullable = true),
    StructField("incident_begin_date", StringType, nullable = true),
    StructField("arrest_date", StringType, nullable = true),
    StructField("felony_review_date", StringType, nullable = true),
    StructField("felony_review_result", StringType, nullable = true),
    StructField("arraignment_date", StringType, nullable = true),
    StructField("updated_offense_category", StringType, nullable = true)
  )
)

// Load sentencing data into sentDF
val sentDF = spark.read.schema(init_schema).option("multiline",true).json("mnicolas/sentencing_*.json")
sentDF.show()
val sentSize = sentDF.count()
println(s"data size: $sentSize")

// Find intersection of ids across the data sets
val commonIDs = initDF.select("case_id").intersect(dispoDF.select("case_id")).intersect(sentDF.select("case_id"))
val commonSize = commonIDs.count()
println(s"data size: $commonSize")

// Specify columns to bring from each DataFrame
val dispoDFSubset = dispoDF.select("case_id", "judge", "chapter", "act", "section", "class", "aoic", "event", "event_date", "incident_end_date", "law_enforcement_agency", "unit", "received_date", "charge_count"
)
val sentDFSubset = sentDF.select("case_id", "case_participant_id", "offense_category", "primary_charge", "charge_id", "charge_version_id", "charge_offense_title", "age_at_incident", "gender", "race", "incident_begin_date", "arrest_date", "arraignment_date", "updated_offense_category", "sentence_judge", "court_name", "court_facility", "sentence_phase", "sentence_date", "sentence_type", "current_sentence", "commitment_type", "commitment_term", "commitment_unit", "length_of_case_in_days", "felony_review_date", "felony_review_result"
)

//
val joinedDF = commonIDs
  .join(dispoDFSubset, Seq("case_id"), "inner")
  .join(sentDFSubset, Seq("case_id"), "inner")

joinedDF.show()