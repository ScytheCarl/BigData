package com.imooc.mr;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Java Serialization compare to Hadoop Serialization
 */

public class JavaSerialize {
    public static void main(String[] args) throws Exception{
        StudentJava studentJava = new StudentJava();
        studentJava.setId(1L);
        studentJava.setName("Hadoop");

        //Write status of Student Object to local
        FileOutputStream fos = new FileOutputStream("/Users/panhaoming/Desktop/StudentJava.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(studentJava);
        oos.close();
        fos.close();
    }
}

class StudentJava implements Serializable {
    private static final long serialVersionUID = 1L;

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
}