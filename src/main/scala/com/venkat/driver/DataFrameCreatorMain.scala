package com.venkat.driver

import com.venkat.process.DataFrameCreator

object DataFrameCreatorMain {

  def main(args: Array[String]): Unit = {

   val obj = new DataFrameCreator()
   obj.createDataFrames()

  }

}
