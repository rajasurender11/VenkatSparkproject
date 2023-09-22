package com.venkat.driver

import com.venkat.process.{RankingDemo, Usecase1}

object RankingDemoMain {

  def main(args: Array[String]): Unit = {

    val obj = new RankingDemo()
    obj.doRankingDemo()

  }
}
