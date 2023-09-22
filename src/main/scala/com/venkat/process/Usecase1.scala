package com.venkat.process
import com.venkat.conf.PropertyConf._
import com.venkat.conf.SparkConf._
import com.venkat.schema.AllSchemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, concat, lit, max, sum, when,count}


class Usecase1 {

  def doUsecase1():Unit = {

    val accountsProfileDF =  spark.read.option("header",true).csv(accountsProfileLoc)
    val atmTransDF =  spark.read.option("delimiter", "|").schema(AllSchemas.atmTransSchema).csv(atmTransLoc)
    accountsProfileDF.createOrReplaceTempView("account_profile")
    atmTransDF.createOrReplaceTempView("atm_trans")
    accountsProfileDF.show()
    atmTransDF.show()
    accountsProfileDF.na.fill("unknown",Seq("bank_name","customer_name"))
    spark.sql("""
                 select a.account_no,
                 CONCAT(customer_name,'|',customer_name) as str // HDFC|surender
                 SUBSTRING(customer_name,2,5) //urend
                 NVL(bank_name,'unknown'),
                 NVL(a.customer_name,'unknown),
                 gender,
                 replace(mobile_no,'9','91')
                 cast(total_withdrawl as string) from
                 account_profile a
                 inner join
                 (select account_no,SUM(trans_amount) as total_withdrawl from atm_trans
                 where status = 'S'
                 group by account_no
                 order by total_withdrawl asc
                 limit 3)b
                 on a.account_no = b.account_no
                 order by total_withdrawl

    """)


    spark.sql("""
                 select a.account_no,
                 bank_name,
                 a.customer_name,
                 NLV(gender,'M'),
                 mobile_no,
                 NVL(total_withdrawl,0) from
                 account_profile a
                 left outer join
                 (select account_no,SUM(trans_amount) as total_withdrawl from atm_trans
                 where status = 'S'
                 group by account_no
                 order by total_withdrawl asc
                 limit 3)b
                 on a.account_no = b.account_no
                 order by total_withdrawl

    """).show(10)


  }

}
