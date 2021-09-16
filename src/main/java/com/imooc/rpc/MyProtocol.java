package com.imooc.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * define a RPC interface
 */

public interface MyProtocol extends VersionedProtocol {
    long versionID = 123456L;
    String Hello(String name);
}
