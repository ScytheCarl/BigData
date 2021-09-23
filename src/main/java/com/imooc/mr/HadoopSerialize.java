package com.imooc.mr;

import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * Java Serialization compare to Hadoop Serialization
 */
public class HadoopSerialize {
    public static void main(String[] args) throws Exception{
        StudentWriable studentWriable = new StudentWriable();
        studentWriable.setId(1L);
        studentWriable.setName("Hadoop");

        //Write status of Student Object to local
        FileOutputStream fos = new FileOutputStream("/Users/panhaoming/Desktop/StudentHadoop.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        studentWriable.write(oos);
        oos.close();
        fos.close();
    }
}

class StudentWriable implements Writable{
    private long id;
    private String name;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.name = in.readUTF();
    }
}