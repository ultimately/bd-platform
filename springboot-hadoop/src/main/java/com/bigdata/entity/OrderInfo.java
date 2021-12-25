package com.hadoop.reduce.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 订单商品对象
 * @author linhaiy
 * @date 2019.05.18
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo implements Writable, Cloneable {
    // 订单号
    private Integer orderId;
    // 时间
    private String orderDate;
    // 产品编号
    private String pid;
    // 数量
    private Integer amount;
    // 产品名称
    private String pname;
    // 种类
    private Integer categoryId;
    // 价格
    private Double price;
    /**
     * 这个字段需要理解<br>
     * 因为这个对象，包含了订单与产品的两个文件的内容，当我们加载一个文件的时候，肯定只能加载一部分的信息，另一部分是加载不到的，需要在join的时候，
     * 加进去，这个字段就代表着这个对象存的是哪些信息 如果为0 则是存了订单信息 如果为1 则是存了产品信息
     */
    private String flag;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(orderId);
        output.writeUTF(orderDate);
        output.writeUTF(pid);
        output.writeInt(amount);
        output.writeUTF(pname);
        output.writeInt(categoryId);
        output.writeDouble(price);
        output.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        orderId = input.readInt();
        orderDate = input.readUTF();
        pid = input.readUTF();
        amount = input.readInt();
        pname = input.readUTF();
        categoryId = input.readInt();
        price = input.readDouble();
        flag = input.readUTF();
    }

    public void set(Integer orderId, String orderDate, String pid, Integer amount, String pname, Integer categoryId,
                    Double price, String flag) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }
}