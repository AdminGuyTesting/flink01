package com.adminguytesting.flink01.FlinkSimpleTests;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
// works
// does read a file with phrases, separates the words, removes the word Apple
// and diplays the words one by one
// docker logs <container> to see the result

public class FlinkStreamSplit03 {

    public void runTest01() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile("/tmp/input.txt");

         DataStream<String> wordsOneByLine= dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out)
                    throws Exception {
                for (String word: value.split(" ")) {
                    out.collect(word);
                }
            }
        });

        DataStream<String> filteredStream = wordsOneByLine.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !(s.equals("Apple")) ;
            }
        });


        System.out.println("Result Stream");
        filteredStream.print();

        env.execute("SplitTest");

    }
}
