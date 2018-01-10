/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: PrintBolt.java
 * Author:   zhangdanji
 * Date:     2017年12月24日
 * Description:
 */
package com.chezhibao.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author zhangdanji
 */
public class PrintBolt implements IRichBolt {

    private static final long serialVersionUID = -8438910244572172230L;
    private OutputCollector collector;
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }
    @Override
    public void execute(Tuple tuple) {
        String print = tuple.getStringByField("print");
        System.out.println(print);
        collector.emit(new Values(print));
    }
    @Override
    public void cleanup() {

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("write"));
    }
}
