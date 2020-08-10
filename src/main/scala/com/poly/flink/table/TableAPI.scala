package com.poly.flink.table

import com.poly.utils.Utils
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.orc.OrcTableSource
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.java.BatchTableEnvironment
import org.apache.hadoop.conf.Configuration

class TableAPI(env: ExecutionEnvironment) {

  val utils: Utils = new Utils()
  val config: Configuration = new Configuration()
  config.set("hadoop.fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
  config.set("hadoop.fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
  config.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
  config.set("spark.hadoop.fs.s3a.fast.upload", "true")

  def batchORC(): Unit = {
    val tableEnv = BatchTableEnvironment.create(env)
    val orc = OrcTableSource
      .builder()
      .path("s3a://poly-testing/covid/orc/combined/", true)
      .withConfiguration(config)
      .build()

    tableEnv.registerTableSource("orcTable", orc)
    val orcTable: Table = tableEnv.sqlQuery(s"select * from orcTable limit 10")
    orc.getDataSet(env).print()
    env.execute()
  }


}
