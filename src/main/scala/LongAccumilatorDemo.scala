package com.demo.sparkExamples

import org.apache.spark.sql.SparkSession

object LongAccumilatorDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Accumulator Demo")
      .master("local")
      .getOrCreate()

    val loncAcc = spark.sparkContext.longAccumulator("SumofAccumulator")

    val rdd = spark.sparkContext.parallelize(Array(1,2,3,4,5))
    rdd.foreach(x => loncAcc.add(x))

    println(loncAcc.value)
    scala.io.StdIn.readLine()
  }

}
