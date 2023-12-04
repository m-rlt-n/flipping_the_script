// Dependencies
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Spark Session
val spark = SparkSession.builder.appName("LoadHiveData").getOrCreate()

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
    StructField("updated_offense_category", StringType, nullable = true),
    StructField("bond_type_current", StringType, nullable = true),
    StructField("bond_amount_current", StringType, nullable = true),
    StructField("bond_electronic_flag_current", StringType, nullable = true)
  )
)

// Load data into initDF
val initDF = spark.read.schema(init_schema).option("multiline",true).json("mnicolas/initiation_*.json")
// initDF.show()
val initSize = initDF.count()
println(s"initDF size: $initSize")

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
    StructField("charge_count", StringType, nullable = true)
  )
)

// Load disposition data into dispoDF
val dispoDF = spark.read.schema(dispo_schema).option("multiline",true).json("mnicolas/disposition_*.json")
// dispoDF.show()
val dispoSize = dispoDF.count()
println(s"dispoDF size: $dispoSize")

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
val sentDF = spark.read.schema(sent_schema).option("multiline",true).json("mnicolas/sentencing_*.json")
// sentDF.show()
val sentSize = sentDF.count()
println(s"sentDF size: $sentSize")

// Find intersection of ids across the data sets
val joinColumns: Seq[String] = Seq("case_id", "case_participant_id", "charge_version_id")
val commonIDs = initDF.selectExpr(joinColumns: _*)
  .intersect(dispoDF.selectExpr(joinColumns: _*))
  .intersect(sentDF.selectExpr(joinColumns: _*))
val commonSize = commonIDs.count()
println(s"commonIDs size: $commonSize")

// Specify columns to bring from each DataFrame
val initDFSubset = initDF.select("case_id", "case_participant_id", "charge_version_id", "updated_offense_category", "bond_type_current", "bond_amount_current", "bond_electronic_flag_current")
val dispoDFSubset = dispoDF.select("case_id", "case_participant_id", "charge_version_id", "chapter", "act", "section", "class", "aoic", "event", "event_date", "law_enforcement_agency", "judge", "unit", "incident_end_date", "received_date", "charge_count")
val sentDFSubset = sentDF.select("case_id", "case_participant_id", "charge_version_id", "charge_id", "offense_category", "primary_charge", "disposition_charged_offense_title", "age_at_incident", "gender", "race", "incident_begin_date", "arrest_date", "arraignment_date", "sentence_judge", "court_name", "court_facility", "sentence_phase", "sentence_date", "sentence_type", "current_sentence", "commitment_type", "commitment_term", "commitment_unit", "length_of_case_in_days", "felony_review_date", "felony_review_result")

// Create Joined DF and save to Hive table
val intermediateJoin1 = commonIDs.join(initDFSubset, joinColumns, "inner")
val intermediateJoin2 = intermediateJoin1.join(dispoDFSubset, joinColumns, "inner")
val joinedDF = intermediateJoin2.join(sentDFSubset, joinColumns, "inner")

//val joinedDF = commonIDs.join(initDFSubset, joinColumns, "inner").join(dispoDFSubset, joinColumns, "inner").join(sentDFSubset, joinColumns, "inner")
joinedDF.show()
val joinedSize = joinedDF.count()
println(s"joinedDF size: $joinedSize")

// Write DataFrame to Hive table
joinedDF.write
  .mode(SaveMode.Overwrite)  // Choose the SaveMode: Overwrite, Append, ErrorIfExists, Ignore
  .saveAsTable("cook_county_data")
// Done