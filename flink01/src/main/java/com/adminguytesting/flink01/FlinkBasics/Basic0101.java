package com.adminguytesting.flink01.FlinkBasics;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDate;

public class Basic0101 {
    public void test01(){
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);


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
        rawCustomers.printExplain();
        String myQuery ="select *";
        env.executeSql(myQuery);

    };



}
