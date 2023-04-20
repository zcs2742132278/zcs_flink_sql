package com.zcs.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author zcs
 * @date 2023/4/20 11:03
 */
public class ZcsConsumer extends Thread {
    //创建消费组对象
    private KafkaConsumer<String,String> consumer;
    /**
     * 创建构造器
     */
    public ZcsConsumer(String cname) {
        super.setName(cname);
//读取配置文件
        Properties properties = new Properties();
//ZK地址
        properties.put("bootstrap.servers",
                "node01:9092,node02:9092,node03:9092");
//消费者所在组的名称
        properties.put("group.id", "yjx-bigdata");
//ZK超时时间
        properties.put("zookeeper.session.timeout.ms", "1000");
//反序列化
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
//当消费者第一次消费时，从最低的偏移量开始消费
        properties.put("auto.offset.reset", "earliest");
//自动提交偏移量
        properties.put("auto.commit.enable", "true");
//消费者自动提交偏移量的时间间隔
        properties.put("auto.commit.interval.ms", "1000");
//创建消费者对象
        consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList("baidu"));
            boolean flag = true;
            while (flag) {
                ConsumerRecords<String, String> records =
                        consumer.poll(100);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords =
                            records.records(partition);
                    for (ConsumerRecord<String, String> record :
                            partitionRecords) {
                        System.out.println(record.topic() + ":" +
                                record.partition() + ":" + record.offset() + ":" + record.key() + ":" +
                                record.value());
                    }
                    consumer.commitSync();//同步
                }
            }
        } finally {
            consumer.close();
        }
    }
    public static void main(String[] args) {
        ZcsConsumer consumer01 = new ZcsConsumer("马龙YYDS");
        consumer01.start();
    }
}
