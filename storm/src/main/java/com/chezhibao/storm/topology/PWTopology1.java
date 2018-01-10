/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: PWTopology1.java
 * Author:   zhangdanji
 * Date:     2017年12月24日
 * Description:
 */
package com.chezhibao.storm.topology;

import com.chezhibao.storm.bolt.PrintBolt;
import com.chezhibao.storm.bolt.WriteBolt;
import com.chezhibao.storm.spout.PWSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author zhangdanji
 */
public class PWTopology1 {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new PWSpout());
        builder.setBolt("print-bolt",new PrintBolt()).shuffleGrouping("spout");
        builder.setBolt("write-bolt",new WriteBolt()).shuffleGrouping("print-bolt");

        //本地模式
        /*LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("top1",config,builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("top1");
        cluster.shutdown();*/

        //集群模式
        StormSubmitter.submitTopology("top1",config,builder.createTopology());
    }
}
