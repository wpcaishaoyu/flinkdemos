package com.peng.demo

import org.apache.flink.streaming.api.scala._

object TextWordCount {
  def main(args: Array[String]): Unit = {
    //first 获取流的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    //获取数据
    print(ClassLoader.getSystemResource(""))
    val data = env.readTextFile(ClassLoader.getSystemResource("") + "word.txt")
    //transformations
    val windowCounts: DataStream[WordWithCount] = data.flatMap(t => t.split("\\s"))
      .map(t => WordWithCount(t, 1))
      .keyBy("word")
      .sum("count")
    // print the results with a single thread, rather than in parallel
    //sink
    //    val wordCount = data.flatMap(t => t.split("\\s")).map(WordWithCount(_, 1)).keyBy("word").sum("count")
    val wordCount = data.flatMap(t => t.split("\\s")).map(WordWithCount(_, 1)).print()
    data.print()
    env.execute("text word count demo")
  }

  // Data type for words with count
  //样例类
  case class WordWithCount(word: String, count: Long)

}
