package com.adminguytesting.flink01.FlinkSinkToFile;
/******************************************************************
 * Reads a file with integers and sends the result of *2 to a file
 * chmod 777 needed on the output dir
 * Exemple qui marche tres bien
 * ****************************************************************/
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.core.fs.Path;

public class FlinkToFile03 {

    public String runTest01() throws Exception {
        StreamExecutionEnvironment myEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        String outputPath = "file:///tmp/data/out/out.txt";
        String inputPath = "file:///tmp/data/in/in.txt";

        DataStream<String> myText = myEnv.readTextFile(inputPath);

        DataStream<Integer> parsed = myText.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) {
                return 2*Integer.parseInt(value);
            }
        });

        parsed.writeAsText(outputPath);

        myEnv.execute();
        return "Done";
    }
}
