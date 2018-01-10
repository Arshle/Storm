/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: WordSpout.java
 * Author:   zhangdanji
 * Date:     2017年12月26日
 * Description:
 */
package com.chezhibao.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangdanji
 */
public class WordSpout implements IRichSpout {

    private static final long serialVersionUID = -3015180011314349024L;
    private SpoutOutputCollector collector;
    private int index = 0;
    private String[] sentences = {
            "my dog has fleas",
            "I like cold beverages",
            "the dog ate my homework",
            "don't hava a cow man",
            "I don't think I like fleas"
    };

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        this.collector.emit(new Values(sentences[index]));
        index ++;
        if(index >= sentences.length){
            index = 0;
        }
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}