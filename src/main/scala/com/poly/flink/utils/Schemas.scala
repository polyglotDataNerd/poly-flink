package com.poly.flink.utils

class Schemas(id: String, metric: Int) extends Serializable {

  def setId(id: String): Unit = {
    val this.id = id
  }

  def getId(): String = {
    this.id
  }

  def setMetric(metric: Int): Unit = {
    val this.metric = metric
  }

  def getMetric(): Int = {
    this.metric
  }


}
