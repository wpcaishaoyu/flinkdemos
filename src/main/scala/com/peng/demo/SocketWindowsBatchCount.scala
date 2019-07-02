package com.peng.demo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SocketWindowsBatchCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //获取数据
    print(ClassLoader.getSystemResource(""))
    val outPut = ClassLoader.getSystemResource("") + "output.txt"
    val text = env.readTextFile(ClassLoader.getSystemResource("") + "word.txt")
    //各中算子的操作和返回值
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1)).groupBy(0)
      .sum(1)
    counts.writeAsCsv(outPut, "\n", " ").setParallelism(1)
    env.execute("batch word count")
  }
}
