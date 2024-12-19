package com.adminguytesting.flink01.ProcessFunctions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;


public class ProcessFunction01 extends ProcessFunction<Input, Output> {

    @Override
    public void processElement(Input input, ProcessFunction<Input, Output>.Context context,
                               Collector<Output> collector) throws Exception {
        //collector.collect(new Output() {});

    }


}
