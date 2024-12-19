package com.adminguytesting.flink01.FlinkTables;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkTables05 {


     public void runTest01(String fileName) throws Exception {
         //filename with format "file:///tmp/cabs/cabs.csv"
            //ParameterTool params = ParameterTool.fromArgs(args);
            EnvironmentSettings settings = EnvironmentSettings
                    .newInstance()
                    .inBatchMode()
                    .build();

         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            //TableEnvironment tableEnv = TableEnvironment.create(settings);
            final Schema schema = Schema.newBuilder()
                    .column("cab_id", DataTypes.INT())
                    .column("cab_plate", DataTypes.STRING())
                    .column("cab_make", DataTypes.STRING())
                    .column("cab_driver", DataTypes.STRING())
                    .column("active_trip", DataTypes.STRING())
                    .column("pickup_location", DataTypes.STRING())
                    .column("target_location", DataTypes.STRING())
                    .column("num_pass", DataTypes.INT())
                    .build();


            tableEnv.createTemporaryTable("cabs",
                    TableDescriptor
                            .forConnector("filesystem")
                            .schema(schema)
                            .option("path", fileName)
                            .format(FormatDescriptor.forFormat("csv").build())
                            .build());




         Table sqlResult = tableEnv.sqlQuery("SELECT * FROM cabs");
         //sqlResult.execute().print();
         //tabEnv.toRetractStream(aggTableSql, Row.class).print("aggTableSql");
         tableEnv.createTemporaryView("myview",sqlResult);
         Table lowerTable = tableEnv.sqlQuery("SELECT LOWER(cab_driver) FROM myview");
         Table lowerTable1 = tableEnv.sqlQuery("SELECT cab_driver FROM myview");
         Table lowerTable2 = tableEnv.sqlQuery("SELECT cab_id FROM myview");
         Table lowerTable3 = tableEnv.sqlQuery("SELECT * FROM myview");
         DataStream<Row> lowerStream = tableEnv.toDataStream(lowerTable);
         DataStream<Row> lowerStream1 = tableEnv.toDataStream(lowerTable1);
         DataStream<Row> lowerStream2 = tableEnv.toDataStream(lowerTable2);
         DataStream<Row> lowerStream3 = tableEnv.toDataStream(lowerTable3);
         lowerStream.print("L");
         lowerStream.print();
         lowerStream1.print();
         lowerStream2.print();
         lowerStream3.print();
         env.execute();

         //Ne marche pas a la compilation
         //Table result = tableEnv.from("cabs").select("*");
         //result.execute().print();
         // *******************************
         // Ne marche pas a l execution
         //TableResult table2 = tableEnv.executeSql("select * from ATHLETES");
         //TableResult result = tableEnv.executeSql("select * from cabs");
         //   result.print();
         // *******************************
        }

}
