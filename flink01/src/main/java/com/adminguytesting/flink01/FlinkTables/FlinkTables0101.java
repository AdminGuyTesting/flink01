package com.adminguytesting.flink01.FlinkTables;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTables0101 {
    public void runTableBasic01(){

        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);

        // Create a TableEnvironment for batch or streaming execution.
        // See the "Create a TableEnvironment" section for details.
        // A Table is always bound to a specific TableEnvironment. It is not possible to combine
        // tables of different TableEnvironments in the same query, e.g., to join or union them.
        // A TableEnvironment is created by calling the static TableEnvironment.create() method.
        //EnvironmentSettings settings = EnvironmentSettings
        //        .newInstance()
        //        .inStreamingMode()
                //.inBatchMode()
        //        .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //create the builder
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.BIGINT().notNull())
                .column("value", DataTypes.STRING())
                .primaryKey("id");


        // Create a source table
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(schemaBuilder.build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());

        // Create a Table object from a Table API query
        //Table table1 = tableEnv.from("SourceTable");
        Table table1 = env.fromValues(
                Row.of(1,"Toto"),
                Row.of(2,"Tata"),
                Row.of(3,"Titi"));

        // Create a sink table (using SQL DDL)
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ");



        // Create a Table object from a SQL query
        //Table table2 = tableEnv.sqlQuery("SELECT * FROM table1");
        Table table2=table1.select($("a"));

        // Emit a Table API result Table to a TableSink, same for SQL result
        TableResult tableResult = table1.insertInto("SinkTable").execute();


    }


}
