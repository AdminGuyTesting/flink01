package com.adminguytesting.flink01.FlinkBasics;

// **********************
// FLINK STREAMING QUERY
// **********************
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TablePipeline;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDate;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.$;

//marche pas
public class Basics02 {
    public void test01(){


        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        //final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment env = TableEnvironment.create(bsSettings);

        //**************************************************************************************
        // create a table with example data without a connector required
        final Table rawCustomers =
                env.fromValues(
                        Row.of(
                                "Guillermo Smith",
                                LocalDate.parse("1992-12-12"),
                                "4081 Valley Road",
                                "08540",
                                "New Jersey",
                                "m",
                                true,
                                0,
                                78,
                                3),
                        Row.of(
                                "Valeria Mendoza",
                                LocalDate.parse("1970-03-28"),
                                "1239  Rainbow Road",
                                "90017",
                                "Los Angeles",
                                "f",
                                true,
                                9,
                                39,
                                0),
                        Row.of(
                                "Leann Holloway",
                                LocalDate.parse("1989-05-21"),
                                "2359 New Street",
                                "97401",
                                "Eugene",
                                null,
                                true,
                                null,
                                null,
                                null),
                        Row.of(
                                "Brandy Sanders",
                                LocalDate.parse("1956-05-26"),
                                "4891 Walkers-Ridge-Way",
                                "73119",
                                "Oklahoma City",
                                "m",
                                false,
                                9,
                                39,
                                0),
                        Row.of(
                                "John Turner",
                                LocalDate.parse("1982-10-02"),
                                "2359 New Street",
                                "60605",
                                "Chicago",
                                "m",
                                true,
                                12,
                                39,
                                0),
                        Row.of(
                                "Ellen Ortega",
                                LocalDate.parse("1985-06-18"),
                                "2448 Rodney STreet",
                                "85023",
                                "Phoenix",
                                "f",
                                true,
                                0,
                                78,
                                3));

        // handle ranges of columns easily
        final Table truncatedCustomers = rawCustomers.select(withColumns(range(1, 7)));

        // name columns
        final Table namedCustomers =
                truncatedCustomers.as(
                        "name",
                        "date_of_birth",
                        "street",
                        "zip_code",
                        "city",
                        "gender",
                        "has_newsletter");

        // register a view temporarily
        env.createTemporaryView("customers", namedCustomers);

        // use SQL whenever you like
        // call execute() and print() to get insights
        /* here here env.sqlQuery(
                        "SELECT "
                                + "  COUNT(*) AS `number of customers`, "
                                + "  AVG(YEAR(date_of_birth)) AS `average birth year` "
                                + "FROM `customers`")
                .execute()
                .print();
        */


        // or further transform the data using the fluent Table API
        // e.g. filter, project fields, or call a user-defined function
        final Table youngCustomers =
                env.from("customers")
                        .filter($("gender").isNotNull())
                        .filter($("has_newsletter").isEqual(true))
                        .filter($("date_of_birth").isGreaterOrEqual(LocalDate.parse("1980-01-01")))
                        .select(
                                $("name").upperCase(),
                                $("date_of_birth"),
                                call(Basics01.AddressNormalizer.class, $("street"), $("zip_code"), $("city"))
                                        .as("address"));
    //*************************************************************************

    // Prepare the insert into pipeline
       TablePipeline pipeline = youngCustomers.insertInto("CsvSinkTable");

    // Print explain details
        pipeline.printExplain();

// emit the result Table to the registered TableSink
       pipeline.execute();
       System.out.println(youngCustomers.explain());

    }
}
