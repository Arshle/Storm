/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: ExclamDrpc.java
 * Author:   zhangdanji
 * Date:     2017年12月26日
 * Description:
 */
package com.chezhibao.storm.drpc;

import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;

/**
 * @author zhangdanji
 */
public class ExclamDrpc {
    public static void main(String[] args) throws TException {
        DRPCClient client = new DRPCClient(null,"follower1",3772);
        for(String word : new String[]{"hello","goodbye"}){
            System.out.println(client.execute("exclamation",word));
        }
    }
}
