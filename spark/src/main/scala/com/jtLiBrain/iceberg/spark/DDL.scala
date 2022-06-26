package com.jtLiBrain.iceberg.spark

import org.apache.spark.sql.SparkSession

object DDL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._



    spark.stop()
  }
}
