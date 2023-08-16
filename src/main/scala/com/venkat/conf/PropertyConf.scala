package com.venkat.conf

object PropertyConf {

  val baseLoc ="C:\\surender\\hadoop_course\\4_inputfiles"
  val accountsProfileLoc = baseLoc + "\\" + "accounts_profile.csv"
  val atmTransLoc = baseLoc + "\\" + "atm_trans.txt"
  val ordersLoc = baseLoc + "\\" + "orders.txt"
  val accountsProfileColList = Seq("account_no", "mobile_no", "customer_name")
  val seqData = Seq(
    ("100","surender","TCS",2000),
    ("101","raja","TCS",3000),
    ("102","kumar","CTS",4000),
    ("103","ajay","CTS",4000),
    ("103","vikram","CTS",4000),
    (null.asInstanceOf[String],"Ashif","IBM",90),
    ("105","Raj",null.asInstanceOf[String],5000),
    ("106","Ankur",null.asInstanceOf[String],6000)
  )
  val empColsList = Seq("emp_id", "emp_name", "emp_company","emp_salary")

  val skillSet = Seq(
    ("100","HADOOP"),
    ("100","SPARK"),
    ("103","AZURE"),
    (null.asInstanceOf[String],"SALESFORCE"),
    ("104","INFORMATICA"),
    ("105","ORACLE")
  )

  val skillsColsList = Seq("emp_id","skills")


}
