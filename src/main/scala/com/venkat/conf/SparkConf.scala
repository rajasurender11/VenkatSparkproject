package com.venkat.conf

import org.apache.spark.sql.SparkSession

object SparkConf {

  val spark = SparkSession.builder
    .appName("BasicSpark")
    .master("local")
    .enableHiveSupport()
    .getOrCreate

  val id = 100

}
