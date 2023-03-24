package com.demo.sparkExamples
import org.apache.spark.sql.SparkSession

object CreateRDD {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MyFirstRDD")
      .master("local[1]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Seq(("Morris",1), ("Kranthi", 1)))

    rdd.foreach(println)

  }

}
