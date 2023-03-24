package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr

object StreamingWC {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StreamingWordCount")
      .master("local[1]")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    //linesDF.printschema()
    val wordDF = linesDF.select(expr("explode(split(value, ' ')) as word"))
    val countDF = wordDF.groupBy("word").count()

    val wordCountQuery = countDF.writeStream
      .format("console")
//      .outputMode("complete")
//      .outputMode("update")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir")
      .start()

    logger.info("Listing to localhost:9999")
    wordCountQuery.awaitTermination()
  }

}
