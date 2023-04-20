package com.zcs.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zcs
 * @date 2023/4/19 9:40
 */
public class ZcsSql01 {
    public static void main(String[] args) {
        /*Configuration configuration = new Configuration();
        TableEnvironment environment = TableEnvironment.create(configuration);
        environment.createTemporaryTable("",
                TableDescriptor.forConnector("")
                        .schema(Schema.newBuilder()
                                .column("","")
                                .build())

                .build());*/

        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //创建Table环境：方法2
        StreamTableEnvironment tableEnvironment1 = StreamTableEnvironment.create(environment);

        //创建Table环境：方法1
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);


    }
}
