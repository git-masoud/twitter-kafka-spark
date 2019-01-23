import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StringType

object TwitterWriter {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder
      .master("local[*]")
      .appName("spark session example")
      .getOrCreate()
    import spark.implicits._
    // Extract
    val records = spark.
      readStream.
      format("kafka").
      option("subscribe", "twitter").
      option("kafka.bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092").
      option("startingoffsets", "latest").
      load
    // Transform
    val result = records
      .withColumn("Key", $"key".cast(StringType))
      .withColumn("Value", $"value".cast(StringType))
      .withColumn("Topic", $"topic".cast(StringType))
    result.writeStream.outputMode("append").format("csv").foreachBatch( (batchDF: DataFrame, batchId: Long) =>
      batchDF.write.format("csv").save(s"/media/masoud/7e99c2c1-c00b-408c-95f2-ac7ddac5617d/bk/twitter_data/$batchId.csv")  // location 1
    ).start().awaitTermination()
    //result.rdd.saveAsTextFile("/media/masoud/7e99c2c1-c00b-408c-95f2-ac7ddac5617d/bk/twitter.csv")
    // Load
//    import org.apache.spark.sql.streaming.{OutputMode, Trigger}
//    import scala.concurrent.duration._
//    val sq = result.
//      writeStream.
//      format("console").
//      option("truncate", false).
//      trigger(Trigger.ProcessingTime(10.seconds)).
//      outputMode(OutputMode.Append).
//      queryName("from-kafka-to-console").
//      start
//
//    // In the end, stop the streaming query
//    sq.stop
//   // df.rdd.saveAsTextFile("/media/masoud/7e99c2c1-c00b-408c-95f2-ac7ddac5617d/bk/twitter")
//
////    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
////      .write
////      .format("kafka")
////      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
////      .option("topic", "topic1")
////      .save()
  }
}
