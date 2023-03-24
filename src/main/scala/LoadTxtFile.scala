package com.demo.sparkExamples

import org.apache.spark.sql.SparkSession

object LoadTxtFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TextFile")
      .master("local[1]")
      .getOrCreate()
      //textFile is Used to read a text file
//    val rdd = spark.sparkContext.textFile("data/textfile.txt")

    //* is Used to read all the text files which we created in particular path we given
    val rdd = spark.sparkContext.wholeTextFiles("data/textfile*")
    rdd.foreach(println)


  }

}
