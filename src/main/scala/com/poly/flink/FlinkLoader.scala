package com.poly.flink

import com.poly.flink.dataset.Sets
import com.poly.flink.datastream.Streams
import com.poly.flink.datatable.Tables
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

      /*batch local environment*/
      //val benv: ExecutionEnvironment = ExecutionEnvironment.createLocalEnvironment(config)
      /*stream local environment*/
      //val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(Runtime.getRuntime.availableProcessors(), config)

      /* run in a flink cluster that's not local i.e. personal laptop */
      val benv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
      val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

      if (args(0).equals("batch")) {
        // basic word count using batch execution reading and s3 file
        new Sets(benv).batchSet(args(1))
      }

      if (args(0).equals("batchtable")) {
        // batch table using BatchTableEnvironment and ORC file as the source
        new Tables(benv, args(1), hadoopConfig).batchORC()
      }

      if (args(0).equals("socketstream")) {
        // socket stream that listens to a host and port with a delimiter
        new Streams(senv).sockets(args(1), args(2).toInt, args(3))
      }

      if (args(0).equals("filestream")) {
        // file stream with s3 path param
        new Streams(senv).files(args(1))
      }
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        System.exit(1)
      }
    }
  }
}
