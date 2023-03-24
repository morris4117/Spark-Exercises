package com.demo.sparkAssignment

import org.apache.spark.sql.SparkSession

object ConvertRDDtoDF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Convert RDD to DF")
      .master("local")
      .getOrCreate()

    val empData = Seq(("Morries", "Data"), ("Hari", "Java"), ("Gopal", "Data"),
      ("Jones", "Devops"), ("Candy", "Java"), ("John", "Devops"), ("Christopher", ".Net"),
      ("Augustine", ".Net"), ("Andrea", "Data"), ("Christiana", "Java"))

    import spark.implicits._
    val columns = Seq("Emp_Name", "Batch")
    val rdd = spark.sparkContext.parallelize(empData)

    val df = empData.toDF(columns:_*)
    df.printSchema()
    df.show()
  }

}
