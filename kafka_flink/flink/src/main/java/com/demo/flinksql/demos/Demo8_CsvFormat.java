package com.demo.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 *        csv format详解
 **/
public class Demo8_CsvFormat {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);


        tenv.executeSql(
                "create table t_csv(                          "
                        + "  id int,                                   "
                        + "  name string,                              "
                        + "  age  string                               "
                        + ") with (                                    "
                        + " 'connector' = 'filesystem',                "
                        + " 'path' = 'data/csv/',                      "
                        + " 'format'='csv',                            "
                        + " 'csv.disable-quote-character' = 'false',    "
                        + " 'csv.quote-character' = '|',                "
                        + "  'csv.ignore-parse-errors' = 'true' ,       "
                        + "  'csv.null-literal' = '\\N'    ,            "
                        + "  'csv.allow-comments' = 'true'             "
                        + ")                                           "
        );

        tenv.executeSql("desc t_csv").print();
        tenv.executeSql("select * from  t_csv").print();

    }

}