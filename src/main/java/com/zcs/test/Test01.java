package com.zcs.test;

import com.zcs.pojo.Emp;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 两种table环境
 *    StreamTableEnvironment(environment)
 *    TableEnvironment.create(setting)
 * 获取表源,进行修改转换
 *    source.map(a->new Object().readValue(a,Emp.class)).flat
 * 创建表
 *    tableEnvironment.fromDataStream()
 * 创建视图
 *    表环境创建("名字",table)
 *
 * @author zcs
 * @date 2023/4/19 15:30
 */
public class Test01 {
    public static void main(String[] args) {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //方法1
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);


        DataStreamSource<String> empStream = environment.readTextFile("data/emp.txt");
        SingleOutputStreamOperator<Emp> map = empStream.map(line -> new ObjectMapper().readValue(line, Emp.class));
        Table table = tableEnvironment.fromDataStream(map);
        table.select(Expressions.$("*"))
                .execute()
                .print();
        table.select(Expressions.$("deptno").isEqual(20),Expressions.$("job"),Expressions.$("ename"))
                .execute()
                .print();
        tableEnvironment.createTemporaryView("abc",table);
        tableEnvironment.executeSql("select * from abc").print();


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment1 = TableEnvironment.create(settings);

    }
}
