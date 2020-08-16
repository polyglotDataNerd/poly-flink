package com.poly.flink.datatable

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.orc.OrcTableSource
import org.apache.flink.table.api.scala.{BatchTableEnvironment, table2RowDataSet}
import org.apache.hadoop.conf.Configuration

class Tables(env: ExecutionEnvironment, source: String, conf: Configuration) {

  def batchORC(): Unit = {
    val tableEnv = BatchTableEnvironment.create(env)
    val orc = OrcTableSource
      .builder()
      .path(source, true)
      .forOrcSchema("struct<name:string,level:string,city:string,county:string,state:string,country:string,population:string,latitude:double,longitude:double,aggregate:string,timezone:string,cases:string,us_confirmed_county:string,deaths:string,us_deaths_county:string,recovered:string,us_recovered_county:string,active:string,us_active_county:string,tested:string,hospitalized:string,discharged:string,last_updated:date,icu:string>")
      .withConfiguration(conf)
      .build()


    orc.getTableSchema
    tableEnv.registerTableSource("orcTable", orc)
    val orcTable = tableEnv.sqlQuery("select * from orcTable where state = 'New York' order by last_updated desc limit 100").collect()
    orcTable.foreach(x=>println(x.toString))
  }

}
