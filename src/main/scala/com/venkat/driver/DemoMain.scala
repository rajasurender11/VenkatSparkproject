package com.venkat.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DemoMain {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("VenkatSpark")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate

    val loc = "/user/training/surender_hadoop/accounts_profile/accounts_profile.txt"
    val rdd = spark.sparkContext.textFile(loc)

    val accountsSchema = StructType(
      Array(
        StructField("account_no", StringType, true),
        StructField("bank_name", StringType, true),
        StructField("cust_name", StringType, true),
        StructField("gender", StringType, true),
        StructField("ph_no", StringType, true)
      )
    )

    val rowRDD = rdd.map(rec => rec.split(",")).map(arr => org.apache.spark.sql.Row(arr: _*))

    val accountsDF = spark.createDataFrame(rowRDD, accountsSchema)

    accountsDF.printSchema()

    // select cust_name, bank_name, ph_no

    val selectDF = accountsDF.select("cust_name", "bank_name", "ph_no")
    selectDF.show(100, false)

    accountsDF.select("cust_name", "bank_name", "ph_no").show(100, false)

    val colsList = List("cust_name","bank_name","ph_no")
    accountsDF.select(colsList.map(c => col(c)): _*).show(100,false)

    accountsDF.createOrReplaceTempView("accounts_table")
    spark.sql("""select  cust_name, bank_name, ph_no from  accounts_table""").show(10,false)
  }

}
