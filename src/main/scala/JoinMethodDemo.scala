package com.demo.sparkAssignment

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

object JoinMethodDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Join Operations")
      .master("local[5]")
      .getOrCreate()

    val df1 = spark.range(1, 5000).toDF("Id").withColumn("cl", lit("NA"))
    val df2 = spark.range(200, 3500).toDF("Id1").withColumn("cl1", lit("NA"))
    //INNER JOIN
//    val df3 = df2.join(df1,df2("Id1") === df1("Id"), "inner")
    //OUTER JOIN
    val df3 = df2.join(df1,df2("Id1") === df1("Id"), "outer")
    println("No.of Partitions: " +df3.rdd.getNumPartitions)
    df3.coalesce(5).write.parquet("Data/Samplejoin")

    scala.io.StdIn.readLine()
  }

}
