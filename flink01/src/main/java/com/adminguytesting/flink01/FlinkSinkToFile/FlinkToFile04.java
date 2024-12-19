package com.adminguytesting.flink01.FlinkSinkToFile;
/******************************************************************
 * Reads from a socket integers and sends the result of *2 to a file
 * chmod 777 needed on the output dir
 * Exemple qui marche tres bien
 * ****************************************************************/
import com.adminguytesting.flink01.FlinkSimpleTests.FlinkTest05;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

public class FlinkToFile04 {

    public String runTest01() throws Exception {
        StreamExecutionEnvironment myEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        String outputPath = "file:///tmp/data/out/out.txt";
        //String inputPath = "file:///tmp/data/in/in.txt";

        //DataStream<String> myText = myEnv.readTextFile(inputPath);

        DataStream<Tuple2<String,Integer>> dataStream = myEnv
                .socketTextStream("localhost", 9999)
                .flatMap(new FlinkToFile04.Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))).max(0);


        DataStream<Integer> parsed = dataStream.map(new MapFunction<Tuple2<String,Integer>,Integer>() {
            @Override
            public Integer map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return 2*stringIntegerTuple2.f1;
            }
        });

        parsed.writeAsText(outputPath);

        myEnv.execute();
        return "Done";
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, Integer.parseInt(word)));
            }
        }
    }


    }
