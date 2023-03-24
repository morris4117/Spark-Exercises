package com.demo.sparkExamples

import org.apache.spark.sql.SparkSession

object DoubleAccumulatorDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Accumulator Demo")
      .master("local")
      .getOrCreate()

    val doubleAcc = spark.sparkContext.doubleAccumulator("")

    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    rdd.foreach(x => doubleAcc.add(x))

    println(doubleAcc.value)
    scala.io.StdIn.readLine()
  }


}
