package com.demo.sparkAssignment

import org.apache.spark.sql.SparkSession

object ConvertJSONtoCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Convert CSV to JSON")
      .master("local")
      .getOrCreate()

    // Data is about Oscar Winning Females from timestamp 1928 to 2016
//    val ConvJSON = spark.read.json("Data/sampleJSON/part-00000-19b88bd3-bd96-47bc-b5cc-b12b80eb856f-c000.json")
    val ConvJSON = spark.read.json()

    ConvJSON.printSchema()
    //    ConvCSV.show()
    ConvJSON.write.csv("Data/OscarCSV")

  }
}
