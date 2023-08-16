package com.venkat.process
import com.venkat.conf.PropertyConf._
import com.venkat.conf.SparkConf._
import com.venkat.schema.AllSchemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class GroupingDemo {

  def groupingDemo():Unit = {
    import spark.implicits._

    val df = spark
      .createDataFrame(seqData)
      .toDF(empColsList:_*)
    //df.show()

  val aggDF =   df.groupBy("emp_company")
                  .agg(
                        count("emp_id").as("total_emps"),
                        sum("emp_salary").as("total_salary"),
                        max("emp_salary").as("max_salary"),
                        min("emp_salary").as("min_salary")
                      )

    val filteredDF = aggDF.filter(col("total_emps") >=3)
    aggDF.filter($"total_emps" >=3)
    aggDF.filter(aggDF("total_emps") >=3)

    df.createOrReplaceTempView("emp")

    spark.sql(
      """
        |select emp_company,
        |COUNT(emp_id) as total_emps,
        |SUM(emp_salary) as total_salary
        |FROM emp
        |GROUP BY emp_company
        |""".stripMargin).show()
     }

}
