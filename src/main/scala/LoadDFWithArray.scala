package com.demo.sparkAssignment

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object LoadDFWithArray {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Array to DF")
      .master("local[1]")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row("James,Smith", List("Java", "Scala", "C++"), List("Spark", "Java"), "OH", "CA"),
      Row("Michael,Rose,", List("Spark", "Java", "C++"), List("Spark", "Java"), "NY", "NJ"),
      Row("Robert,,Williams", List("CSharp", "VB"), List("Spark", "Python"), "UT", "NV"))

    val arrayStructureSchema = new StructType()
      .add("name", StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("languagesAtWork", ArrayType(StringType))
      .add("currentState", StringType)
      .add("previousState", StringType)

    val df = spark.createDataFrame(
//    df.printSchema
    spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)
//    df.show()
