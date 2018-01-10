/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: WordCountBolt.java
 * Author:   zhangdanji
 * Date:     2017年12月26日
 * Description:
 */
package com.chezhibao.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangdanji
 */
public class WordCountBolt implements IRichBolt {

    private static final long serialVersionUID = 8346258725326936351L;
    private OutputCollector collector;
    private HashMap<String,Long> counts = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if(count == null){
            count = 0L;
        }
        count ++;
        this.counts.put(word,count);
        this.collector.emit(new Values(word,count));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
