package com.adminguytesting.flink01.KeyedProcessFunction;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.util.Collector;

public class KeyedProcessFunction01 extends KeyedProcessFunction<String, Input, Output> {
    @Override
    public void processElement(Input input, KeyedProcessFunction<String, Input, Output>.Context context, Collector<Output> collector) throws Exception {
        String key = context.getCurrentKey();

    }
}
