package com.venkat.schema

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object AllSchemas {

  val accountsSchema = StructType(
    Array(
      StructField("account_no", StringType, true),
      StructField("bank_name", StringType, true),
      StructField("cust_name", StringType, true),
      StructField("gender", StringType, true),
      StructField("ph_no", StringType, true)
    )
  )

  val atmTransSchema = StructType(Array(
    StructField("account_no", StringType, true),
    StructField("atm_id", StringType, true),
    StructField("trans_date", StringType, true),
    StructField("trans_amount", IntegerType, true),
    StructField("status", StringType, true)
  ))

  val ordersSchema = StructType(Array(
    StructField("order_date", StringType, true),
    StructField("cust_name", StringType, true),
    StructField("order_id", StringType, true),
    StructField("product", StringType, true),
    StructField("trans_mode", IntegerType, true),
    StructField("product_price", IntegerType, true)
  ))


}
