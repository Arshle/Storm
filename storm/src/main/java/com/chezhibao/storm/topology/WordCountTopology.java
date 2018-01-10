/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: WordCountTopology.java
 * Author:   zhangdanji
 * Date:     2017年12月26日
 * Description:
 */
package com.chezhibao.storm.topology;

import com.chezhibao.storm.bolt.SplitBolt;
import com.chezhibao.storm.bolt.WordCountBolt;
import com.chezhibao.storm.bolt.WordReportBolt;
import com.chezhibao.storm.spout.WordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangdanji
 */
public class WordCountTopology {

    private final static String WORD_SPOUT_ID = "word-spout";
    private final static String SPLIT_BOLT_ID = "split-bolt";
    private final static String COUNT_BOLT_ID = "count-bolt";
    private final static String REPORT_BOLT_ID = "report-bolt";
    private final static String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws InterruptedException {
        //构建topology
        TopologyBuilder builder = new TopologyBuilder();
        //spout
        builder.setSpout(WORD_SPOUT_ID,new WordSpout());
        //WordSpout -> WordSplitBolt
        builder.setBolt(SPLIT_BOLT_ID,new SplitBolt(),5).shuffleGrouping(WORD_SPOUT_ID);
        //WordSplitBolt -> WordCountBolt
        builder.setBolt(COUNT_BOLT_ID,new WordCountBolt(),5).fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"));
        //WordCountBolt -> WordReportBolt
        builder.setBolt(REPORT_BOLT_ID,new WordReportBolt(),10).globalGrouping(COUNT_BOLT_ID);
        //本地配置
        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
        TimeUnit.SECONDS.sleep(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
