package com.peng.demo

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object WordCount {
  def main(args: Array[String]): Unit = {
    // the port to connect to

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 8001, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    val res: JobExecutionResult = env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
