package com.peng.demo;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCountDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        DataStream<String> text = env.socketTextStream("localhost", 9000, '\n', 23);
        DataStream<WordWithCount1> wordWithCount = text.flatMap(new FlatMapFunction<String, WordWithCount1>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount1> collector) throws Exception {
                String[] splits = s.split("\\s");
                for (String word : splits) {
                    collector.collect(new WordWithCount1(word, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("count");
        wordWithCount.print().setParallelism(4);
        env.execute();

    }

    public static class WordWithCount1 {
        public String word;
        public long count;

        public WordWithCount1() {
        }

        public WordWithCount1(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}