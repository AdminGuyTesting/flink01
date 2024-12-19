package com.adminguytesting.flink01.FlinkBasics;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;

import java.time.LocalDate;

import static org.apache.flink.table.api.Expressions.*;
//marche pas
//https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/tableapi/
// prendre une table en donnees harcodees et l envoyer dans un sink
public class Basics03 {
    public void test01(){

        //EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        //TableEnvironment tEnv = TableEnvironment.create(settings);

        final Schema schema = Schema.newBuilder()
                .column("count", DataTypes.INT())
                .column("word", DataTypes.STRING())
                .build();

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

// register Orders table in table environment
// ...
        final Table orders =
                tEnv.fromValues(
                        Row.of("Books",
                                "Furnitures",
                                "Books"));

// specify table program
        //Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

        Table counts = orders
                .groupBy($("a"))
                .select($("a"), $("b").count().as("cnt"));

// print
        counts.execute().print();

    }
}
