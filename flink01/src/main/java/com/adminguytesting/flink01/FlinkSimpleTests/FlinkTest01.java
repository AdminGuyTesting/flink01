package com.adminguytesting.flink01.FlinkSimpleTests;

import com.adminguytesting.flink01.FlinkSinkToFile.FlinkToFile01;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlinkTest01 {
    private static final Logger log = LoggerFactory.getLogger(FlinkTest01.class);

    public void runtest01() throws Exception{
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        	env.fromElements(1,2,3,4,5).print();

        	env.execute();

    }

    public void runTest02() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1L, 2L, 3L, 4L, 3L, 2L, 1L, 0L)
                .keyBy(x -> 1)
                .process(new KeyedProcessFunction<Integer, Long, List<Long>> () {
                    private transient MapState<Long, Boolean> set;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        set = getRuntimeContext().getMapState(new MapStateDescriptor<>("set", Long.class, Boolean.class));
                    }

                    @Override
                    public void processElement(Long x, Context context, Collector<List<Long>> out) throws Exception {
                        if (set.contains(x)) {
                            System.out.println("set contains " + x);
                            log.info("set contains " + x);
                        } else {
                            System.out.println("set did not contain " + x);
                            System.out.println("Let s add "+x+" to our data set. ");
                            log.info("set did not contain " + x);
                            log.info("Let s add "+x+" to our data set. ");
                            set.put(x, true);
                            List<Long> list = new ArrayList<>();
                            Iterator<Long> iter = set.keys().iterator();
                            iter.forEachRemaining(list::add);
                            out.collect(list);
                        }
                    }
                })
                .print();

        env.execute();

    }


    public void runTest03(){
       // DataStream<YourDataType> stream = // your Flink data stream
       //         stream.addSink(new FlinkToFile01<>());
       // DataStream<Tuple2<String, Integer>> dataStream =

    }

}
