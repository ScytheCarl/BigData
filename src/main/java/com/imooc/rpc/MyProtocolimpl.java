package com.imooc.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * RPC Implementation class
 */

public class MyProtocolimpl implements MyProtocol{

    /**
     *
     * @param name
     * @return
     */
    public String Hello(String name) {
        System.out.println("Nice, Implemented.....");
        return "hello " + name;
    }

    /**
     *
     * @param s
     * @param l
     * @return
     * @throws IOException
     */
    public long getProtocolVersion(String s, long l) throws IOException {
        return versionID;
    }

    /**
     *
     * @param s
     * @param l
     * @param i
     * @return
     * @throws IOException
     */
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature();
    }
}
