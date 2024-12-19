package com.adminguytesting.flink01.ProcessFunctions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ProcessFunction03UseCase {
    public void exemple01() throws Exception{
        Tuple2<String, String> myList = new Tuple2<String,String>("hello","holle");

        //StreamExecutionEnvironment env =
        //        StreamExecutionEnvironment.getExecutionEnvironment();
        //env.fromElements(myList).print();

        //env.execute();

        // the source data stream
       // DataStream<Tuple2<String, String>> stream = new DataStream<>(myList);


// apply the process function onto a keyed stream
        //DataStream<Tuple2<String, Long>> result = stream
        //        .keyBy(value -> value.f0)
        //        .process(new ProcessFunction03());

    }
}
