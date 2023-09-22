package com.venkat.process
import com.venkat.conf.PropertyConf._
import com.venkat.conf.SparkConf._
import com.venkat.schema.AllSchemas

class RankingDemo {

  def doRankingDemo():Unit ={

    val accountsProfileDF =  spark.read.option("header",true).csv(accountsProfileLoc)
    val atmTransDF =  spark.read.option("delimiter", "|").schema(AllSchemas.atmTransSchema).csv(atmTransLoc)
    accountsProfileDF.createOrReplaceTempView("account_profile")
    atmTransDF.createOrReplaceTempView("atm_trans")
    spark.sql("""
        |select b.* from
        |(select account_no, bank_name, trans_date, trans_amount,
        |rank() over(partition by bank_name order by trans_amount desc ) as rank_number
        |from
        |(select  account_no,
        |split(atm_id,':')[0] as bank_name,
        |trans_date,
        |trans_amount
        |from atm_trans )a
        |)b
        |where b.rank_number =1""".stripMargin)


spark.sql("""select * from
        |(select  account_no,
         |split(atm_id,':')[0] as bank_name,
         |trans_date,
       |trans_amount,
       |dense_rank() over(partition by split(atm_id,':')[0] order by trans_amount desc ) as rank_number
        |from atm_trans )a
        """.stripMargin)


    spark.sql("""select * from
                |(select  account_no,
                |split(atm_id,':')[0] as bank_name,
                |trans_date,
                |trans_amount,
                |row_number() over(partition by split(atm_id,':')[0] order by trans_amount desc ) as rank_number
                |from atm_trans )a
        """.stripMargin)


    val colsarr = atmTransDF.columns
    colsarr.foreach(println)


  }

}
