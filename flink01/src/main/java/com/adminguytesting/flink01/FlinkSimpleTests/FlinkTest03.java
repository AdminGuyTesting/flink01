package com.adminguytesting.flink01.FlinkSimpleTests;
//https://www.baeldung.com/apache-flink


import com.adminguytesting.flink01.UserClasses.MyProduct;
import org.apache.commons.collections.ArrayStack;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class FlinkTest03 {
    public void runtest01() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> amounts = env.fromElements(1, 29, 40, 50, 60,70,92,101);

        List<Integer> liste1 = new ArrayList<Integer>();
        liste1.add(4);
        liste1.add(14);
        liste1.add(24);
        liste1.add(34);

        List<Integer> liste2 = new ArrayList<Integer>();
        liste2.add(11);
        liste2.add(21);
        liste2.add(31);
        liste2.add(41);

        Tuple2<Integer, Integer> zippedLists = new Tuple2<>();
        for (Integer i=0;i<liste1.toArray().length;i++) {
            //zippedLists.
        }
         //       Flux.zip(characterFlux, foodFlux);


        //DataSet<Tuple2<Integer, Integer>> intPairs = ([1,2],[3,4],[5,6]);
        //        DataSet<Integer> intSums = intPairs.map(new IntAdder());

    }

    // MapFunction that adds two integer values
    public class IntAdder implements MapFunction<Tuple2<Integer, Integer>, Integer> {
        @Override
        public Integer map(Tuple2<Integer, Integer> in) {
            return in.f0 + in.f1;
        }
    }
}
