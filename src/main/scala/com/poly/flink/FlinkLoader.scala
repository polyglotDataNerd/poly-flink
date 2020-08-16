package com.poly.flink

import com.poly.utils.{ConfigProps, Utils}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.core.plugin.PluginUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

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
    try {
      val hadoopConfig: org.apache.hadoop.conf.Configuration = new org.apache.hadoop.conf.Configuration()
      hadoopConfig.set("fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
      hadoopConfig.set("fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
      hadoopConfig.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
      hadoopConfig.set("fs.s3a.fast.upload", "true")
      hadoopConfig.set("orc.compress", "SNAPPY")

      val config: org.apache.flink.configuration.Configuration = new org.apache.flink.configuration.Configuration()
      config.setString("s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
      config.setString("s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
      config.setString("s3a.endpoint", "s3.us-west-2.amazonaws.com")
      config.setString("s3a.fast.upload", "true")
      config.setInteger("parallelism", Runtime.getRuntime.availableProcessors())
      /*
      needs to set flink-conf.yaml with AWS keys to run local with ENV Variable
      https://stackoverflow.com/questions/48460533/how-to-set-presto-s3-xxx-properties-when-running-flink-from-an-ide
       */

      FileSystem.initialize(GlobalConfiguration.loadConfiguration(System.getenv("FLINK_CONF_DIR")), PluginUtils.createPluginManagerFromRootFolder(config))

      /*batch environment*/
      val benv: ExecutionEnvironment = ExecutionEnvironment.createLocalEnvironment(config)
      /*stream environment*/
      val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(Runtime.getRuntime.availableProcessors(), config)

      /*
        // basic word count using batch execution reading and s3 file
        runFlink(benv)

        // socket stream that listens to a host and port with a delimiter
        new Streams(senv).sockets("127.0.0.1", 9000, "\n")

        // file stream with s3 path param
        new Streams(senv).files("s3a://poly-testing/covid/combined/covid19_combined.gz")

        // batch table using BatchTableEnvironment and ORC file as the source
        new TableAPI(benv, "s3a://poly-testing/covid/orc/combined/", hadoopConfig).batchORC()
       */
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        System.exit(1)
      }
    }
  }
}
