import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// 
val spark = SparkSession.builder.appName("CountyDataTransform").getOrCreate()

// initiation schema
val schema = StructType(
  List(
    StructField("column1", StringType, nullable = true),
    StructField("column2", IntegerType, nullable = true),
    // Add more StructFields for each column in your data
  )
)

// 
val initDF = spark.read.json("mnicolas/initiation_*.json")
