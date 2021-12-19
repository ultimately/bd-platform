package com.bigdata.entity;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName User
 * @Description:
 * @Author levlin
 * @Date 2021/12/9
 **/
@Data
public class User implements Writable {

    private String username;
    private Integer age;
    private String address;

    public User() {
        super();
        // TODO Auto-generated constructor stub
    }

    public User(String username, Integer age, String address) {
        super();
        this.username = username;
        this.age = age;
        this.address = address;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 把对象序列化
        dataOutput.writeChars(username);
        dataOutput.writeInt(age);
        dataOutput.writeChars(address);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 把序列化的对象读取到内存中
        username = dataInput.readUTF();
        age = dataInput.readInt();
        address = dataInput.readUTF();
    }
}
