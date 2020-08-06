package com.poly.flink

import org.apache.flink.api.common.operators._
import org.apache.flink.api.scala._

/**
 * Created by gbartolome on 08/05/2020.
 * Flink Scala EMR Shell https://docs.aws.amazon.com/emr/latest/ReleaseGuide/flink-scala.html
 *
 */
object Loader extends java.io.Serializable {

  def main(args: Array[String]): Unit = {
    runFlink()
  }

  def runFlink(): Unit = {
    val benv = ExecutionEnvironment.getExecutionEnvironment
    benv.getConfig.getDefaultKryoSerializers

    val input = benv
      .readTextFile("s3a://poly-testing/covid/combined/")
      //\w+ matches one or more word characters
      .flatMap(_.toLowerCase().replaceAll("\"", "").split("\\W+"))
      .filter(_.nonEmpty)
      .map(x => (x, 1))
      .groupBy(0)
      .sum("_2")
      .sortPartition(1,Order.DESCENDING)
      .first(10)
    //input.writeAsCsv("s3://poly-testing/covid/flink/", "\n", "\t")
    input.print()

    benv.execute("Word Count")

  }
}
