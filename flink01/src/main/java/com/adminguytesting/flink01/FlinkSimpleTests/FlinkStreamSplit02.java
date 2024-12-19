package com.adminguytesting.flink01.FlinkSimpleTests;

// works
// does read a file with phrases and diplays the words one by one
// docker logs <container> to see the result

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class FlinkStreamSplit02 {

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

        System.out.println("Source Stream");
        wordsOneByLine.print();
        env.execute("SplitTest");

    }
}
