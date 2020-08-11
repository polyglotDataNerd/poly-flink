package com.poly.flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.orc.OrcTableSource
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.hadoop.conf.Configuration

class TableAPI(env: ExecutionEnvironment, source: String, conf: Configuration) {

  def batchORC(): Unit = {
    val tableEnv = BatchTableEnvironment.create(env)
    val orc = OrcTableSource
      .builder()
      .path(source, true)
      .withConfiguration(conf)
      .build()

    tableEnv.registerTableSource("orcTable", orc)
    val orcTable: Table = tableEnv.sqlQuery(s"select * from orcTable limit 10")
    orc.getDataSet(env.asInstanceOf[org.apache.flink.api.java.ExecutionEnvironment]).print()
    env.execute()
  }


}
