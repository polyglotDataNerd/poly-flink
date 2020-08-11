package com.poly.flink

import com.poly.flink.table.TableAPI
import com.poly.utils.{ConfigProps, Utils}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.hadoop.conf.Configuration

/**
 * Created by gbartolome on 08/05/2020.
 * Flink Scala EMR Shell https://docs.aws.amazon.com/emr/latest/ReleaseGuide/flink-scala.html
 *
 */
object FlinkLoader extends java.io.Serializable {
  val appConfig: ConfigProps = new ConfigProps()
  /*set logger*/
  System.setProperty("logfile.name", "/var/tmp/flink.log")
  appConfig.loadLog4jprops()
  val utils: Utils = new Utils()

  def main(args: Array[String]): Unit = {
    val config: Configuration = new Configuration()
    config.set("hadoop.fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
    config.set("hadoop.fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
    config.set("hadoop.fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
    config.set("hadoop.fs.s3a.fast.upload", "true")

    val benv = ExecutionEnvironment.createLocalEnvironment()
    //runFlink(benv)
    new TableAPI(benv, "s3a://poly-testing/covid/orc/combined/", config).batchORC()
  }

  def runFlink(bEnv: ExecutionEnvironment): Unit = {
    val input = bEnv
      .readTextFile("s3://poly-testing/covid/combined/")
      //\w+ matches one or more word characters
      .flatMap(_.toLowerCase().replaceAll("\"", "").split("\\W+"))
      .filter(_.nonEmpty)
      .map(x => (x, 1))
      .groupBy(0)
      .sum("_2")
      .sortPartition(1, Order.DESCENDING)
      .first(10)
    //input.writeAsCsv("s3://poly-testing/covid/flink/", "\n", "\t")
    input.print()

    bEnv.execute("Word Count")

  }
}
