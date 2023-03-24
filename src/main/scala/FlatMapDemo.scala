package com.demo.sparkExamples

import org.apache.spark.sql.SparkSession

object FlatMapDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MapMethod")
      .master("local[1]")
      .getOrCreate()

    val data = Seq("Project Guitenberg's",
      "Alice's Adventures in Wonderland",
      "Maruthi King in data team NexTurn")

    import spark.sqlContext.implicits._
    val df = data.toDF("data")
    df.show()

    val mapDF = df.flatMap(fun => {
      fun.getString(0).split(" ")
    })
    mapDF.show()
  }
}
