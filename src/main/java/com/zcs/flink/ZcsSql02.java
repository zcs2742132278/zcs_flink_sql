package com.zcs.flink;

import com.zcs.pojo.Emp;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.DriverManager;

/**
 * 原子(基础数据类型)
 * Tuple
 * POJO
 * ROW
 *
 * @author zcs
 * @date 2023/4/19 10:28
 */
public class ZcsSql02 {
    public static void main(String[] args) {
        /*//运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //source
        DataStreamSource<String> deptSource = environment.readTextFile("data/dept.txt");
        DataStreamSource<String> empSource = environment.readTextFile("data/emp.txt");

        //建表  方法1
        Table table = tableEnvironment.fromDataStream(deptSource);
        table.select(Expressions.$("*")).execute().print();

        //转换
        SingleOutputStreamOperator<Emp> empStream = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));
        //建表   方法2
        Table table2 = tableEnvironment.fromDataStream(empStream);
        table2.select(Expressions.$("*")).execute().print();


        //创建视图  方法1
        tableEnvironment.createTemporaryView("t_emp",table2.select(Expressions.$("empno")));
        tableEnvironment.executeSql("select * from t_emp").print();

        //创建视图  方法2
        tableEnvironment.createTemporaryView("t_dept",table);
        tableEnvironment.executeSql("select * from t_dept").print();*/

//        zcsFunction();
        zcsTransformationFunction();
    }



    /**
     * pojo+查询表
     */
    private static void zcsFunction() {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //数据源pojo
        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");
        //表源
        DataStream<Emp> tableSource = source.map(a -> new ObjectMapper().readValue(a, Emp.class));
        //创建表  fromDataStream
        Table table = tableEnvironment.fromDataStream(tableSource);
        //查找打印表
        table.select(Expressions.$("*")).execute().print();
        table.select(Expressions.$("*"))
                .groupBy(Expressions.$("deptno"))
                        .select(Expressions.$("deptno"),Expressions.$("sal").sum().as("sumSal"))
                .where(Expressions.$("deptno").isEqual(30))
                                .execute().print();
        //表查询
        tableEnvironment.sqlQuery("select * from "+table.toString()).execute().print();
        //表修改
        //表插入
//        table.executeInsert("insert into ");
        //创建视图，以前版本
//        tableEnvironment.registerDataStream("a",tableSource);
//        DriverManager.getConnection("").createStatement().execute("")

        //创建视图,表环境创建
        tableEnvironment.createTemporaryView("ac",table);
        tableEnvironment.executeSql("select * from ac").print();
        tableEnvironment.executeSql("select deptno,sum(sal) sumSal from ac where deptno = 30 group by deptno").print();



    }

    /**
     *  输出表
     *  表流转换
     */
    private static void zcsTransformationFunction() {

    }
}
