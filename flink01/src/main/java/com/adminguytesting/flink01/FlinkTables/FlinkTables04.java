package com.adminguytesting.flink01.FlinkTables;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;


public class FlinkTables04 {
    public void runTests01() {

        // create a TableEnvironment for batch or streaming execution
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // create an input Table
        TableResult tempResult = tableEnv.executeSql(
                "create temporary table ATHLETES (" +
                //"create table ATHLETES (" +
                        "name varchar," +
                        "country varchar," +
                        "sport varchar) with (" +
                        "'connector' = 'filesystem'," +
                        "'path' = 'file:///tmp/t_foo1.csv','format'='csv')");

        TableResult table2 = tableEnv.executeSql("select * from ATHLETES");

    }
}
