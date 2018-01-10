/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: BasicDrpcTopology.java
 * Author:   zhangdanji
 * Date:     2017年12月26日
 * Description:
 */
package com.chezhibao.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author zhangdanji
 */
public class BasicDrpcTopology{

    public static class ExaimBolt implements IBasicBolt{

        private static final long serialVersionUID = 5858232143035620106L;

        @Override
        public void prepare(Map map, TopologyContext topologyContext) {

        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String input = tuple.getString(1);
            basicOutputCollector.emit(new Values(tuple.getValue(0),input + "!"));
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","result"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExaimBolt(),3);
        Config config = new Config();
        if(args == null || args.length == 0){
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("drpc-demo",config,builder.createLocalTopology(drpc));
            for(String word : new String[]{"hello","goodbye"}){
                System.out.println("Result for \"" + word + "\":" + drpc.execute("exclamation",word));
            }
            cluster.shutdown();
            drpc.shutdown();
        }else{
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0],config,builder.createRemoteTopology());
        }
    }
}
