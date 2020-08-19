package com.poly.flink.datatable

import com.poly.flink.utils.Schemas
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.orc.OrcTableSource
import org.apache.flink.table.api.bridge.scala._
import org.apache.hadoop.conf.Configuration

class Tables(env: ExecutionEnvironment, source: String, conf: Configuration) extends Serializable {

  private val schemas: Schemas = new Schemas()

  def batchORC(): Unit = {
    val tableEnv = BatchTableEnvironment.create(env)
    val orc = OrcTableSource
      .builder()
      .path(source, true)
      .forOrcSchema(schemas.getCovidSchema().getSchema.toString)
      .withConfiguration(conf)
      .build()


    orc.getTableSchema
    tableEnv.registerTableSource("orcTable", orc)
    val orcTable = tableEnv.sqlQuery("select * from orcTable where state = 'New York' order by last_updated desc limit 100").collect()
    orcTable.foreach(x => println(x.toString))
  }

}
