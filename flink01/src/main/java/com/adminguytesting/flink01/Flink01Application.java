package com.adminguytesting.flink01;

import com.adminguytesting.flink01.Avro.ParquetAvroHandler;
import com.adminguytesting.flink01.CreateData.CreateData01;
import com.adminguytesting.flink01.FlinkBasics.Basics01;
import com.adminguytesting.flink01.FlinkBasics.Basics02;
import com.adminguytesting.flink01.FlinkBasics.Basics03;
import com.adminguytesting.flink01.FlinkSimpleTests.*;
import com.adminguytesting.flink01.FlinkSinkToFile.FlinkToFile02;
import com.adminguytesting.flink01.FlinkSinkToFile.FlinkToFile03;
import com.adminguytesting.flink01.FlinkSinkToFile.FlinkToFile04;
import com.adminguytesting.flink01.FlinkTables.FlinkTables04;
import com.adminguytesting.flink01.FlinkTables.FlinkTables05;
import com.adminguytesting.flink01.FlinkTables.FlinkTables06;
import com.adminguytesting.flink01.ProcessFunctions.SideOutputExample;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.Op;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Flink01Application{

	private static final Logger logger = LoggerFactory.getLogger(Flink01Application.class);

	public static void main(String[] args) throws Exception {

		//System.out.println("lets start");
		logger.info("Lets start !");

		//log1.info("lets start with an info message");
		//log1.warn("Lets fire a warning message");
		//log1.error("Let s fire an error message");

		//FlinkTest01 flinkTest01 = new FlinkTest01();
		//FlinkTest02 flinkTest02 = new FlinkTest02();
		//FlinkTest04 flinkTest04 = new FlinkTest04();
		//FlinkTest05 flinkTest05 = new FlinkTest05();
		//FlinkToFile02 flinkToFile02 = new FlinkToFile02();
		//FlinkToFile03 flinkToFile03 = new FlinkToFile03();
		//FlinkToFile04 flinkToFile04 = new FlinkToFile04();

		//flinkTest01.runTest02();
		//flinkTest02.runTest01();
		//flinkTest04.runTest1();

		//flinkTest05.runTest01();
		//flinkToFile03.runTest01();
		//flinkToFile04.runTest01();

		//SideOutputExample soe = new SideOutputExample();
		//String[] a = {};
		//soe.runMain(a);

		//Basics02 basics02 = new Basics02();
		//basics02.test01();

		//Basics03 basics03 = new Basics03();
		//basics03.test01();

		//FlinkTables04 flinkTables04 = new FlinkTables04();
		//flinkTables04.runTests01();

		//en cours
		//CreateData01 createData01 = new CreateData01();
		//String filename="/tmp/cabs.csv";
		//createData01.createFileInProvidedFullPath(filename);
		//FlinkTables05 flinkTables05 = new FlinkTables05();
		//String filename2="file:///tmp/cabs.csv";
		//flinkTables05.runTest01(filename2);

		//FlinkStreamSplit01 flinkStreamSplit01 = new FlinkStreamSplit01();
		//flinkStreamSplit01.runTest01();;
//		FlinkStreamSplit03 flinkStreamSplit03 = new FlinkStreamSplit03();
//		flinkStreamSplit03.runTest01();;

		//FlinkTables05 flinkTables05 = new FlinkTables05();
		//String filename2="file:///tmp/cabs.csv";
		//flinkTables05.runTest01(filename2);

		//FlinkTables06 flinkTables06 = new FlinkTables06();
		//flinkTables06.runTest02();
		ParquetAvroHandler parquetAvroHandler = new ParquetAvroHandler();
		parquetAvroHandler.runTest01();

		String myMessage2="Job finished";
		//System.out.println(message2);
		logger.info(myMessage2);

		//log1.info("Job finished");

	}

}