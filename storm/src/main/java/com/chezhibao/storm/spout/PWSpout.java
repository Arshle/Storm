/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: PWSpout.java
 * Author:   zhangdanji
 * Date:     2017年12月24日
 * Description:
 */
package com.chezhibao.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author zhangdanji
 */
public class PWSpout implements IRichSpout {

    private static final long serialVersionUID = -6888653791419101224L;
    private SpoutOutputCollector collector;

    private static final Map<Integer, String> MAP = new HashMap<>();

    static{
        MAP.put(0,"java");
        MAP.put(1,"php");
        MAP.put(2,"groovy");
        MAP.put(3,"python");
        MAP.put(4,"ruby");
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
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }
    @Override
    public void nextTuple() {
        final Random r = new Random();
        int num = r.nextInt(5);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.collector.emit(new Values(MAP.get(num)));
    }
    @Override
    public void ack(Object o) {

    }
    @Override
    public void fail(Object o) {

    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("print"));
    }
}
