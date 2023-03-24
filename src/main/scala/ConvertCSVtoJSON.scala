package com.demo.sparkAssignment

import org.apache.spark.sql.SparkSession

object ConvertCSVtoJSON {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Convert CSV to JSON")
      .master("local")
      .getOrCreate()
    // Data is about Oscar Winning Females from timestamp 1928 to 2016
    val ConvCSV = spark.read.option("header", true).csv("C:/Users/Morris/Downloads/oscar_age_female.csv")

    ConvCSV.printSchema()
//    ConvCSV.show()
    ConvCSV.toJSON.write.json("Data/sampleJSON")

  }

}
