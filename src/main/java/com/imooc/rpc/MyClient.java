package com.imooc.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * Client Code
 */
public class MyClient {
    public static void main(String[] args) throws Exception{
        //Connect to RPC Server through Socket
        InetSocketAddress addr = new InetSocketAddress("localhost", 1234);

        Configuration conf = new Configuration();
        //Get RPC proxy
        MyProtocol proxy = RPC.getProxy(MyProtocol.class, MyProtocol.versionID, addr, conf);

        String result = proxy.Hello("RPC");
        System.out.println("RPC Client Recieved Result:" + result);
    }
}

