package com.poly.flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.orc.OrcTableSource
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.hadoop.conf.Configuration

class TableAPI(env: ExecutionEnvironment, source: String, conf: Configuration) {

  def batchORC(): Unit = {
      val tableEnv = BatchTableEnvironment.create(env)
    val orc = OrcTableSource
      .builder()
      .path(source, true)
      .forOrcSchema("struct<name:string,level:string,city:string,county:string,state:string,country:string,population:string,latitude:double,longitude:double,aggregate:string,timezone:string,cases:string,us_confirmed_county:string,deaths:string,us_deaths_county:string,recovered:string,us_recovered_county:string,active:string,us_active_county:string,tested:string,hospitalized:string,discharged:string,last_updated:date,icu:string>")
      .withConfiguration(conf)
      .build()

    tableEnv.registerTableSource("orcTable", orc)
    val orcTable: Table = tableEnv.sqlQuery("select * from orcTable order by last_updated desc limit 10")
    orcTable.getSchema
    tableEnv.execute("job_test")
    env.executeAsync("job_test2")
  }

}
