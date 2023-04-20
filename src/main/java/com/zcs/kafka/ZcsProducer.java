package com.zcs.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author zcs
 * @date 2023/4/20 11:02
 */
public class ZcsProducer extends Thread {
    //创建生产者
    private Producer<String, String> producer;

    /**
     * 创建构造器
     *
     * @param producerName
     */
    public ZcsProducer(String producerName) {
        //设置线程名字
        super.setName(producerName);
        //创建配置文件列表
        Properties properties = new Properties();

        // kafka地址，多个地址用逗号分割
        properties.put("bootstrap.servers",
                "node01:9092,node02:node03:9092");
//设置写出数据的格式
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
//写出的应答方式
        properties.put("acks", "all");
//错误重试
        properties.put("retries", 1);
//批量写出
        properties.put("batch.size", 16384);
//创建生产者对象
        producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void run() {
        //初始化一个计数器
        int count = 0;
        System.out.println("Hello01Producer.run--开始发送数据");
//迭代發送消息
        while (count < 100) {
            count++;
            String key = "zcs01-" + count;
            String value = Thread.currentThread().getName() + "--" +
                    count;
//封装消息对象
            ProducerRecord<String, String> producerRecord = new
                    ProducerRecord<>("baidu", key, value);
//发送消息到服务器
            producer.send(producerRecord);
//打印消息
//System.out.println("Producer.run--" + key + "--" + value);
//每个1秒发送1条
//try {
// Thread.sleep(1000);
//} catch (InterruptedException e) {
// e.printStackTrace();
//}
        }
//强行将数据写出
        producer.close();
        System.out.println("ZcsProducer.run--发送数据完毕");
    }

    public static void main(String[] args) {
        ZcsProducer producer = new ZcsProducer("zcs01");
        producer.start();
    }
}
