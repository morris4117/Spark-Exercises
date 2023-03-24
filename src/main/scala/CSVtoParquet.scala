package com.demo.sparkAssignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}

object CSVtoParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Convert CSV to JSON")
      .master("local")
      .getOrCreate()
    // Data is about Oscar Winning Females from timestamp 1928 to 2016
    val ConvPrqt = spark.read.csv("C:/Users/Morris/Downloads/oscar_age_female.csv")

    ConvPrqt.printSchema()
    ConvPrqt.write.parquet("Data/SamplePrqt")
  }

}
