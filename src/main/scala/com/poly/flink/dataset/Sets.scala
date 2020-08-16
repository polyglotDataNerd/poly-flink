package com.poly.flink.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

class Sets(bEnv: ExecutionEnvironment) {

  def batchSet(sourcePath: String): Unit = {
    try {
      val input = bEnv
        .readTextFile(sourcePath)
        //\w+ matches one or more word characters
        .flatMap(_.toLowerCase().replaceAll("\"", "").split("\\W+"))
        .filter(_.nonEmpty)
        .map(x => (x, 1))
        .filter(_._1.contains("york"))
        .groupBy(0)
        .sum("_2")
        .sortPartition(1, Order.DESCENDING)
        .first(10)
      //input.writeAsCsv("s3://poly-testing/covid/flink/", "\n", "\t")
      input.print()
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        System.exit(1)
      }
    }

  }
}
