package com.demo.sparkExamples

import org.apache.spark.sql.SparkSession

object CollectionAccumulator {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Accumulator Demo")
      .master("local")
      .getOrCreate()

    val collectAcc = spark.sparkContext.collectionAccumulator[Int]("Collect Accumulator")

    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    rdd.foreach(x => collectAcc.add(x))

    println(collectAcc.value)
    scala.io.StdIn.readLine()
  }


}
