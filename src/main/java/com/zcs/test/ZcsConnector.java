package com.zcs.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * kafkaSource
 * kafkaSink
 * jdbc
 *
 * @author zcs
 * @date 2023/4/19 17:47
 */
public class ZcsConnector {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

//        zcsKafkaSource(environment,tableEnvironment);
//        zcsKafkaSink(environment,tableEnvironment);
        zcsKafkaJdbc(environment,tableEnvironment);


    }

    private static void zcsKafkaJdbc(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        String jdbc="CREATE TABLE books (\n" +
                "  id BIGINT,\n" +
                "  title STRING,\n" +
                "  authors STRING,\n" +
                "  book_year INT \n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/zcs?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n"+
                "   'username' = 'root',\n"+
                "   'password' = '123456',\n"+
                "   'table-name' = 'books'\n" +
                ");";
        //执行sql
        tableEnvironment.executeSql(jdbc);
        //查询结果
        tableEnvironment.sqlQuery("select * from books").execute().print();
    }

    /**
     *   kafkaSink
     *
     * @param environment
     * @param tableEnvironment
     * @throws Exception
     */
    private static void zcsKafkaSink(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) {
        //创建表的SQL语句
        String kafkaSink="CREATE TABLE flink_kafka_sink (\n" +
                "  `deptno` INT,\n" +
                "  `loc` STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_kafka_sink',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'zcs_test',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
        //执行sql
        tableEnvironment.executeSql(kafkaSink);
        //查询结果
        tableEnvironment.executeSql("insert into flink_kafka_sink select deptno+1000,loc from flink_kafka_source");
    }

    /**
     *   kafkaSource
     *
     * @param environment
     * @param tableEnvironment
     * @throws Exception
     */
    private static void zcsKafkaSource(StreamExecutionEnvironment environment, StreamTableEnvironment tableEnvironment) throws Exception {
        //创建表的SQL语句kafka_source
        String kafkaSource="CREATE TABLE flink_kafka_source (\n" +
                "  `deptno` INT,\n" +
                "  `dname` STRING,\n" +
                "  `loc` STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_kafka_source',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'zcs_test',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";
        //执行SQL
        tableEnvironment.executeSql(kafkaSource);
        //查询结果   和sink，哪个在前，先执行哪个
//        tableEnvironment.sqlQuery("select * from flink_kafka_source ").execute().print();
    }
}
