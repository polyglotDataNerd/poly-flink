package com.poly.flink.utils

import org.apache.orc.TypeDescription
import org.apache.orc.mapred.OrcStruct

class Schemas() extends Serializable {

  def getCovidSchema(): OrcStruct = {
    val schemaString: String = "struct<name:string,level:string,city:string,county:string,state:string,country:string,population:string,latitude:double,longitude:double,aggregate:string,timezone:string,cases:string,us_confirmed_county:string,deaths:string,us_deaths_county:string,recovered:string,us_recovered_county:string,active:string,us_active_county:string,tested:string,hospitalized:string,discharged:string,last_updated:date,icu:string>"
    val schema: TypeDescription = TypeDescription.fromString(schemaString)
    new OrcStruct(schema)
  }
}
