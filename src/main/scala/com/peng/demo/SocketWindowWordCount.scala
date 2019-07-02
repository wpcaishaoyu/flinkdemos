package com.peng.demo

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 9000, '\n')
    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      //每隔一秒，统计最近五秒的数据,窗口计算
      .timeWindow(Time.seconds(5), Time.seconds(1))
      //      .sum("count")
      .reduce((a, b) => WordWithCount(a.word, a.count + b.count))
    windowCounts.print().setParallelism(2)
    //提交job
    env.execute("socket windows word count")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
