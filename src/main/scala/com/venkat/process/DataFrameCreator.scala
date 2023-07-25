package com.venkat.process

import com.venkat.conf.SparkConf._
import spark.sqlContext.implicits._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

class DataFrameCreator {


def createDataFrames(): Unit = {

  //collection of tuples, then it is  easy to create direct DF
  val seqData = Seq(
    ("100","surender",2000),
    ("101","raja",3000),
    ("102","kumar",4000),
    ("103","kumar",4000),
    ("103","mmmmmmmmmmmmmmmmmmmmmmmmmmmmaaaaaaaaa",4000)
  )
  val df1 = seqData.toDF("emp_id","emp_name","salary")
  //df1.show()
  //df1.printSchema()

  val rdd = spark.sparkContext.parallelize(seqData)
  val colsList = List("id","name")
  //rdd.toDF().show()
  //rdd.toDF(colsList:_*).show()


  val structSchema = StructType( Array(
    StructField("my_id", StringType,true),
    StructField("my_name", StringType,true)
  ))
  val rowRDD = rdd.map(tuple => Row(tuple._1, tuple._2))
  val df3 = spark.createDataFrame(rowRDD,structSchema)
  //df3.show(10,false)


  val listData = List(
    "100,surender,chennai",
    "101,ajay,bangalore"
  )
  /*
  List(
  Array(100,surender,chennai),
  Array(101,ajay,bangalore)
  )

  List(
  Row(100,surender,chennai),
  Row(101,ajay,bangalore)
  )
   */

  val empSchema = StructType( Array(
    StructField("my_id", StringType,true),
    StructField("my_name", StringType,true),
    StructField("my_city", StringType,true)
  ))

  val myRDD = spark.sparkContext.parallelize(listData)
  val myRowRDD = myRDD.map(elem => elem.split(","))
                      .map(arr => Row(arr:_*))

  val df4 = spark.createDataFrame(myRowRDD,empSchema)
  //df4.show()

  val hdfsLoc ="C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
  val df5 = spark.read.option("header",true).csv(hdfsLoc)

  val atmTransLoc ="C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"
  val atmTransSchema = StructType(Array(
    StructField("account_no", StringType, true),
    StructField("atm_id", StringType, true),
    StructField("trans_date", StringType, true),
    StructField("amount", IntegerType, true),
    StructField("status", StringType, true)
  ))
  val df6 = spark.read.option("delimiter", "|").schema(atmTransSchema).csv(atmTransLoc)
  df6.show()


  }



}
