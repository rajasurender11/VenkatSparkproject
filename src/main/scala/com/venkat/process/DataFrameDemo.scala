package com.venkat.process
import com.venkat.conf.PropertyConf._
import com.venkat.conf.SparkConf._
import com.venkat.schema.AllSchemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}


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

    //doShow(sqlDF)
    accountsProfileDF.filter(col("bank_name") === "HDFC" )
    doShow(accountsProfileUpdatedDF)
    //accountsProfileDF.select("customer_name", "bank_name").show()
    val selectedDF = accountsProfileDF.select(accountsProfileColList.map(elem => col(elem)):_*)
    selectedDF.show()

    val updatedDF = accountsProfileDF.withColumnRenamed("mobile_no", "contact_no")
    accountsProfileDF.printSchema()
    updatedDF.printSchema()

    val addedDF = accountsProfileDF.withColumn("contact_no",col("mobile_no"))
    addedDF.show()

    addedDF.drop("mobile_no")
    val unionedDF  = accountsProfileDF.union(sqlDF).union(sqlDF)
    unionedDF.distinct()
    unionedDF.dropDuplicates()

   val cityDF = spark.sql(
      """
        |select account_no , bank_name, customer_name, gender,
        |case when bank_name in ('HDFC','SBI') then 'CHN'
        |when bank_name = 'CITI' then 'BNG'
        |else 'MUM'
        |end city
        |
        |from accounts_profile
        |""".stripMargin)

    val cityUpdatedDF = accountsProfileDF
      .withColumn("city",when(col("bank_name").isin(List("HDFC","SBI"):_*),"CHN")
                                  .when(col("bank_name").isin(List("CITI"):_*),"BNG")
                                  .otherwise("MUM")
                 )
        cityUpdatedDF.show()
  }



  def doShow(df:DataFrame):Unit = {

    df.show(10,false)

  }

}
