package com.demo.sparkAssignment

import org.apache.spark.sql.SparkSession

object PartitionMethod {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Create Partition")
      .master("local[5]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Range(0,20))
    println("From local[5]: "+rdd.partitions.size)

//    val rdd1 = spark.sparkContext.parallelize(Range(0, 20), 6)
//    println("Parallelize: " + rdd1.partitions.size)
//
//    val rdd2 = spark.sparkContext.textFile("Data/textfile.txt", 4)
//    println("TextFile: " + rdd2.partitions.size)

    rdd.coalesce(5).saveAsTextFile("Data/partition")
//    rdd1.saveAsTextFile("Data/partition1")
//    rdd2.saveAsTextFile("Data/partition2")
    scala.io.StdIn.readLine()
  }

}
