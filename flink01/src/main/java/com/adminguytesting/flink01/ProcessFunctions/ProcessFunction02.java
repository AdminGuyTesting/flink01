package com.adminguytesting.flink01.ProcessFunctions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.util.Collector;

public class ProcessFunction02  extends ProcessFunction<Input[], Output>  {
    //@Override
    //public void processElement(Input[] collection, ProcessFunction<Input, Output>.Context context,
   //                            Collector<Output> collector) throws Exception {
        //for (Input input:collection){
        //    collector.collect(new Output(input));
        //}

    //}

    @Override
    public void processElement(Input[] inputs, ProcessFunction<Input[], Output>.Context context, Collector<Output> collector) throws Exception {
        //for (Input input:inputs){
            //collector.collect(new Output(input));
            //collector.collect(input.toString());
        //}
    }
}
