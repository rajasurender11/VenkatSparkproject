package com.venkat.process

import com.venkat.conf.PropertyConf.{empColsList, seqData, skillSet, skillsColsList}
import com.venkat.conf.SparkConf.spark

class JoinDemo {

def joinDemo(): Unit ={

  import spark.implicits._

  val empDF = spark
    .createDataFrame(seqData)
    .toDF(empColsList:_*)

  val skillsDF = spark
    .createDataFrame(skillSet)
    .toDF(skillsColsList:_*)

  empDF.createOrReplaceTempView("employee")
  skillsDF.createOrReplaceTempView("skills")

 val joinedDF = spark.sql(
   """
     | SELECT e.emp_id as emp_id,
     | emp_name,
     | emp_company,
     | skills
     | FROM
     | employee e
     | INNER JOIN
     | skills s
     | ON(e.emp_id != s.emp_id)
     |""".stripMargin)
  joinedDF.show()
  val jDF = empDF.alias("e")
    .join(skillsDF.alias("s"),
    empDF("emp_id")=== skillsDF("emp_id"),
    "inner")
    .select("s.emp_id","emp_name","emp_company","skills")


  val leftJoinedDF = spark.sql(
    """
      | SELECT e.emp_id as emp_id,
      | emp_name,
      | emp_company,
      | skills
      | FROM
      | employee e
      | LEFT OUTER JOIN
      | skills s
      | ON(e.emp_id = s.emp_id)
      |""".stripMargin)

  val antiJoinedDF = spark.sql(
    """
      | SELECT e.emp_id as emp_id,
      | emp_name,
      | emp_company
      | FROM
      | employee e
      | ANTI JOIN
      | skills s
      | ON(e.emp_id = s.emp_id)
      |""".stripMargin)


}




}
