import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put

object StreamCases {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val scoredCaseData = hbaseConnection.getTable(TableName.valueOf("nmarlton_cook_county_data_vo"))

  def addCaseRecord(record: KafkaCaseRecord): Put = {
    val put = new Put(Bytes.toBytes(record.case_id))
    put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("case_id"), Bytes.toBytes(record.case_id))
    put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("offense_category"), Bytes.toBytes(record.offense_category))
    put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("offense_title"), Bytes.toBytes(record.disposition_charged_offense_title))
    put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("received_date"), Bytes.toBytes("2021-12-07"))
    put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("judge"), Bytes.toBytes(record.judge))
    put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("predicted_risk_percentile"), Bytes.toBytes("50"))

    put
  }

  // TODO: Add call to spark ML files to one hot encode incoming records and produce scores
  // See batch_layer/02_prep_for_serving/create_hive_table.scala for sample code reading and apply SparkML models

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: StreamFlights <brokers>
                            |  <brokers> is a list of one or more Kafka brokers
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamFlights")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("county_cases")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val kcrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaCaseRecord]))
    kcrs.print()

    // Write to HBase
    kcrs.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create())
        val table = connection.getTable(TableName.valueOf("nmarlton_cook_county_data_v0"))
        partition.foreach { record =>
          val put = addCaseRecord(record)
          table.put(put)
        }
        table.close()
        connection.close()
      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}