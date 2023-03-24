package com.demo.sparkExamples

import org.apache.spark.sql.SparkSession

object BroadcastDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("BroadCast Demo")
      .master("local[1]")
      .getOrCreate()

    val inputrdd = spark.sparkContext.parallelize(Seq(("Emp1", "1000", "USA", "NY"), ("Emp2", "2000", "IND", "TS"),
      ("Emp3", "3000", "IND", "AP"), ("Emp4", "7000", "AUS", "QUE"), ("Emp5", "6000", "USA", "TX"), ("Emp6", "2000", "IND", "TN")))
//    inputrdd.foreach(println)

    val states = Map(("NY", "New York"), ("TS", "Telangana"), ("AP", "Andhra Pradesh"), ("QUE", "Queens"), ("TX", "Texis"), ("TN", "Tamilnadu"))
    val countries = Map(("USA","United States of America"), ("IND", "India"), ("AUS", "Australia"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val finalRDD = inputrdd.map(f=>{
      val country = f._3
      val state = f._4

      val fullstate = broadcastStates.value(state)
      val fullcountry = broadcastCountries.value(country)
      (f._1,f._2, fullcountry, fullstate)
    })

    println(finalRDD.collect().mkString("\n"))
    scala.io.StdIn.readLine()
  }

}
