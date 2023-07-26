package com.venkat.process
import com.venkat.conf.PropertyConf._
import com.venkat.conf.SparkConf._
import com.venkat.schema.AllSchemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}


class DataFrameDemo {

  def doDemo():Unit={

    val accountsProfileDF =  spark.read.option("header",true).csv(accountsProfileLoc)
    val atmTransDF =  spark.read.option("delimiter", "|").schema(AllSchemas.atmTransSchema).csv(atmTransLoc)
    val ordersDF = spark.read.schema(AllSchemas.ordersSchema).csv(ordersLoc)
//    doShow(accountsProfileDF)
//    doShow(atmTransDF)
//    doShow(ordersDF)
    val accountsProfileUpdatedDF = accountsProfileDF.withColumn("city", lit("Chennai"))

    accountsProfileDF.createOrReplaceTempView("accounts_profile")

  val sqlDF =spark.sql(
    """select * from accounts_profile where bank_name = 'HDFC'
      |
      |""".stripMargin)

    doShow(sqlDF)
    accountsProfileDF.filter(col("bank_name") === "HDFC" )
    doShow(accountsProfileUpdatedDF)
    accountsProfileDF.select("customer_name", "bank_name").show()

  }



  def doShow(df:DataFrame):Unit = {

    df.show(10,false)

  }

}
