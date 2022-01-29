package com.venkat.driver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, lower}
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

    accountsDF.filter(col("bank_name") === "HDFC" or col("bank_name") === "SBI" ).show(10,false)
    spark.sql("""select * from accounts_table where  bank_name = 'HDFC' """).show(10,false)

    spark.sql("""select  account_no, bank_name, cust_name, gender, ph_no as mobile_num from accounts_table  """).show(100,false)
    val updatedDF =  accountsDF.withColumnRenamed("ph_no" ,"mobile_number").drop("account_no")
    updatedDF.show(10)

    val addedDF = accountsDF.withColumn("mobile_number",col("ph_no")).withColumn("account_type",lit("savings")).withColumn("bank_name",lower(col("bank_name")))

    val df1 = accountsDF
    val df2 = accountsDF

    df1.union(df2).distinct().show(100,false)

    val empLoc = "/user/training/surender_hadoop/employee/"
    val skillsLoc = "/user/training/surender_hadoop/skills/skills.txt"

    val empSchema = StructType(
      Array(
        StructField("emp_id", StringType, true),
        StructField("emp_name", StringType, true)
      )
    )

    val skillsSchema = StructType(
      Array(
        StructField("id", StringType, true),
        StructField("skills", StringType, true)
      )
    )

    val empRDD = spark.sparkContext.textFile(empLoc)
    val skillsRDD = spark.sparkContext.textFile(skillsLoc)

    val empRowRDD = empRDD.map(rec => rec.split("\\|")).map(arr => org.apache.spark.sql.Row(arr: _*))
    val skillsRowRDD = skillsRDD.map(rec => rec.split(",")).map(arr => org.apache.spark.sql.Row(arr: _*))

    val empDF = spark.createDataFrame(empRowRDD, empSchema)
    val skillsDF = spark.createDataFrame(skillsRowRDD, skillsSchema)

    empDF.createOrReplaceTempView("employees")
    skillsDF.createOrReplaceTempView("skills")

    val joinedDF1 = spark.sql(""" select * from employees inner join skills on(employees.emp_id = skills.id)""")

    val joinedDF2 = spark.sql(""" select emp_id, emp_name, skills from employees inner join skills on(employees.emp_id = skills.id)""")

    val joinedDF3 = spark.sql(""" select emp_id, emp_name, skills from employees inner join skills on(employees.emp_id = skills.id) where skills in ('BIGDATA','ORACLE')""")

    val joinedDF4 = spark.sql(""" select * from employees left outer join skills on(employees.emp_id = skills.id) """)

    val joinedDF5 = spark.sql(""" select * from employees right outer join skills on(employees.emp_id = skills.id) """)

    val joinedDF6 = spark.sql(""" select * from employees full outer join skills on(employees.emp_id = skills.id) """)

    val joinedDF7 = spark.sql(""" select * from employees left anti join skills on(employees.emp_id = skills.id) """)

    val joinedDF8 = spark.sql(""" select emp_id,emp_name from employees left outer join skills on(employees.emp_id = skills.id) where skills.id is null """)

  }

}
