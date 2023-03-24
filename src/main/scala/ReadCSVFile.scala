package com.demo.sparkExamples

import org.apache.spark.sql.SparkSession

object ReadCSVFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("toCSV")
      .master("local")
      .getOrCreate()

        val dfFromCSV = spark.read.option("header", true).csv("data/Emp.csv")
//    val dfFromCSV = spark.read.csv("data/Emp.csv")

    dfFromCSV.printSchema()
    dfFromCSV.show()
  }

}
