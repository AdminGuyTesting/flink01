package com.adminguytesting.flink01.StreamCollectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamCollector01 {
    //https://www.digitalocean.com/community/tutorials/java-stream-collect-method-examples
    public void collector01(){
        List<String> vowels = List.of("a", "e", "i", "o", "u");

// sequential stream - nothing to combine
        StringBuilder result = vowels.stream().collect(StringBuilder::new, (x, y) -> x.append(y),
                (a, b) -> a.append(",").append(b));
        System.out.println(result.toString());

// parallel stream - combiner is combining partial results
        StringBuilder result1 = vowels.parallelStream().collect(StringBuilder::new, (x, y) -> x.append(y),
                (a, b) -> a.append(",").append(b));
        System.out.println(result1.toString());

        String result2 = vowels.parallelStream()
                .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                .toString();

    }

    //Stream collect() to List using Collectors Class
    public void collector02(){
        List<Integer> numbers = List.of(1, 2, 3, 4, 5, 6);

        List<Integer> evenNumbers = numbers.stream().filter(x -> x % 2 == 0).collect(Collectors.toList());
        System.out.println(evenNumbers);  // [2, 4, 6]

    }

    //Stream collect() to a Set
    public void collector03(){
        List<Integer> numbers = List.of(1, 2, 3, 4, 5, 6);

        Map<Integer, String> mapOddNumbers = numbers.parallelStream().filter(x -> x % 2 != 0)
                .collect(Collectors.toMap(Function.identity(), x -> String.valueOf(x)));
        System.out.println(mapOddNumbers); // {1=1, 3=3, 5=5}
    }


    //Collectors joining() Example
    public void collector04(){
        String value = Stream.of("a", "b", "c").collect(Collectors.joining());
        //value ==> "abc"

        String valueCSV = Stream.of("a", "b", "c").collect(Collectors.joining(","));
        //valueCSV ==> "a,b,c"

        String valueCSVLikeArray = Stream.of("a", "b", "c").collect(Collectors.joining(",", "{", "}"));
        //valueCSVLikeArray ==> "{a,b,c}"

        String valueObject = Stream.of("1", new StringBuffer("2"), new StringBuilder("3")).collect(Collectors.joining());
        //valueObject ==> "123"

    }


    //reduce
   // public void collector05(){
   //     DataStream<Tuple2<String, Integer>> itemIdsAndCount;
    //    DataStream<Tuple2<String,Integer>> wordCountsByFirstLetter = itemIdsAndCount.keyBy(tuple -> tuple.f0)
    //            .reduce((l1,l2) -> new Tuple2(l1.f0,l1.f1+l2.f1));
    //}
}
