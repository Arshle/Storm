/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: KafkaConsumer.java
 * Author:   zhangdanji
 * Date:     2017年12月27日
 * Description:
 */
package com.chezhibao.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author zhangdanji
 */
public class KafkaConsumer {

    private final static String TOPIC = "message";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "follower1:2181,follower2:2181,follower3:2181");
        properties.put("group.id","group1");
        properties.put("zookeeper.session.timeout.ms","4000");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "200");
        properties.put("auto.offset.reset","smallest");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(TOPIC, 1);
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        Map<String,List<KafkaStream<String,String>>> consumerMap = consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(TOPIC).get(0);
        for (MessageAndMetadata<String, String> aStream : stream) {
            System.out.println(aStream.message());
        }
    }
}
