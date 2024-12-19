package com.adminguytesting.flink01.FlinkSimpleTests;

import com.adminguytesting.flink01.UserClasses.MyProduct;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;

import java.util.ArrayList;
import java.util.List;

public class FlinkTest02 {
    public void runTest01() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(200L);
        List<MyProduct> objects = myProductList();
        DataStream<MyProduct> stream = env.fromCollection(objects);
        stream./*
    timeWindowAll(Time.seconds(5)).reduce(new Reduce()).*/
                print();
        env.execute("Flink Streaming Java Quickstart Mio");
    }

    private static List<MyProduct> myProductList() {
        List<MyProduct> objects = new ArrayList<>();
        int prop1 = 0;
        int prop2 = 1000;
        String stringa1 = "stringa1: " + prop1;
        String stringa2 = "stringa2: " + prop2;
        for (int i = 0; i < 1000; i++) {
            prop1 = prop1 + 1;
            prop2 = prop2 + 1;
            stringa1 = "stringa1: " + prop1;
            stringa2 = "stringa2: " + prop2;
            MyProduct o = new MyProduct(prop1, prop2, stringa1, stringa2);
            objects.add(o);
        }
        return objects;
    }

    private static class Reduce implements org.apache.flink.api.common.functions.ReduceFunction<MyProduct> {
        @Override
        public MyProduct reduce(MyProduct p1, MyProduct t1) throws Exception {
            if (p1.getPrice1() <= t1.getPrice1()) {
                return p1;
            } else {
                return t1;
            }
        }
    }
}
