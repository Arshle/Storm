/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: WriteBolt.java
 * Author:   zhangdanji
 * Date:     2017年12月24日
 * Description:
 */
package com.chezhibao.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.FileWriter;
import java.util.Map;

/**
 * @author zhangdanji
 */
public class WriteBolt implements IRichBolt {

    private static final long serialVersionUID = 6784099940723431880L;
    private FileWriter fileWriter;
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }
    @Override
    public void execute(Tuple tuple) {
        String write = tuple.getStringByField("write");
        System.out.println(write);
        try {
            if(fileWriter == null){
                fileWriter = new FileWriter(new File("/Users/zhangdanji/Desktop/write.txt"));
            }
            fileWriter.write(write);
            fileWriter.write("\n");
            fileWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void cleanup() {

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
