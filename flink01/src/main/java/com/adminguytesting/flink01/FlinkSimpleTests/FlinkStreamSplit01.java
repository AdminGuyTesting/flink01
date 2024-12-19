package com.adminguytesting.flink01.FlinkSimpleTests;
// fourre tout , tests .
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class FlinkStreamSplit01 {

    public void runTest01() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile("/tmp/input.txt");
        List<String> sep = new ArrayList<String>();
        sep.add("Next Stream");
        DataStream<String> separator =env.fromData(sep);
        DataStream<String> stream1 = dataStream.filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String s) throws Exception {
                    //List<String> output = new ArrayList<String>();
                    if (s.equals("Apple")) {
                        return false;
                    } else {
                        return true;
                    }
                }
            }

        );

        DataStream<String> stream2 = stream1.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
            //List<String> output = new ArrayList<String>();
            return !(s.equals("Orange")) ;
            }
            }
        );


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

        //separator.print();

        //System.out.println("No Apple Stream");
        //stream1.print();
        //separator.print();

        //System.out.println("No Apple no Orange Stream");
        //stream2.print();
        //separator.print();
        //separator.sinkTo("This is the separator");
        env.execute("SplitTest");

    }
}
