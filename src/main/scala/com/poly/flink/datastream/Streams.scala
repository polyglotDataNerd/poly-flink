package com.poly.flink.datastream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

class Streams(env: StreamExecutionEnvironment) extends Serializable {

  def sockets(host: String, port: Int, delim: String): Unit = {
    val socketStream = env.socketTextStream(host, port, delim.toCharArray.head)

    val wordCountWindow = socketStream
      .flatMap(
        //split regex by white space
        x => x.split("\\s")
      )
      .map(w => (w, 1))
      .keyBy("_1")
      .timeWindow(Time.seconds(3), Time.seconds(1))
      .sum("_2")

    //print check without Parallelism
    wordCountWindow.print().setParallelism(1)
    env.executeAsync("word count sockets")
  }

  def files(path: String): Unit = {
    val fileStream = env.readTextFile(path)

    val wordCountWindow = fileStream
      .flatMap(
        //split regex by white space
        x => x.split("\\s")
      )
      .map(w => (w, 1))
      .keyBy("_1")
      .timeWindow(Time.seconds(3), Time.seconds(1))
      .sum("_2")

    //print check without Parallelism
    wordCountWindow.print().setParallelism(1)
    env.executeAsync("word count sockets")
  }

}
