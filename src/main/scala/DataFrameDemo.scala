package com.demo.sparkExamples

import org.apache.spark.sql.SparkSession

object DataFrameDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFrame Demo")
      .master("local")
      .getOrCreate()

    val data = Seq(("Morris", "90000", "21", "Hyderabad"),
      ("Mary Jones", "78000", "17", "Gangtok"),
      ("Manvitha", "100000", "19", "Banglore"),
      ("Lavanya", "18000", "40", "Chirala"),
      ("Mojes Babu", "800000", "38", "Banglore"))

    import spark.implicits._
    val columns = Seq("Name", "Salary", "Age", "Location")
    val rdd = spark.sparkContext.parallelize(data)

    val df = rdd.toDF(columns:_*)
//    df.printSchema()
    df.show()
  }

}
