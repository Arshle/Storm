/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: KafkaProducer.java
 * Author:   zhangdanji
 * Date:     2017年12月27日
 * Description:
 */
package com.chezhibao.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangdanji
 */
public class KafkaProducer {

    private static final String TOPIC = "message";

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("zookeeper.connect","follower1:2181,follower2:2181,follower3:2181");
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list","follower1:9092");
        properties.put("request.required.acks","1");
        Producer<Integer,String> producer = new Producer<>(new ProducerConfig(properties));
        for(int i = 0; i < 10; i ++){
            producer.send(new KeyedMessage<Integer,String>(TOPIC,"hello kafka" + i));
            System.out.println("send Message : " + "hello kafka " + i);
            TimeUnit.SECONDS.sleep(1);
        }
        producer.close();
    }
}
