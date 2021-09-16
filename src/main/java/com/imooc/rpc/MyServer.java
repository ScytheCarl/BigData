package com.imooc.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * server code
 */

public class MyServer {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //create RPC server builder
        RPC.Builder builder = new RPC.Builder(conf);
        builder.setBindAddress("localhost") //set listener IP or hostname
            .setPort(1234) //set port
            .setProtocol(MyProtocol.class) //set rpc interface
            .setInstance(new MyProtocolimpl());

        //Build RPC Server
        RPC.Server server = builder.build();

        server.start();
        System.out.println("RPC SERVER LAUNCHED....");
    }
}
