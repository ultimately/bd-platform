package com.bigdata.hadoop.reduce.mapper;

import lombok.SneakyThrows;
import org.apache.hadoop.io.NullWritable;
import com.hadoop.reduce.model.OrderInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName JoinReduce
 * @Description:
 * @Author levlin
 * @Date 2021/12/25
 **/
public class JoinReduce extends Reducer<Text, OrderInfo, OrderInfo, NullWritable> {

    private final static String ORDER_FLAG = "0";
    private final static String PRODUCT_FLAG = "1";

    @SneakyThrows
    @Override
    public void reduce(Text text, Iterable<OrderInfo> values, Context context) {
        OrderInfo orderInfo = new OrderInfo();

        List<OrderInfo> list = new ArrayList<>();
        for (OrderInfo info : values) {
            // 判断是订单还是产品的map输出
            if (ORDER_FLAG.equals(info.getFlag())) {
                // 订单表数据
                OrderInfo tmp = new OrderInfo();
                try {
                    tmp = (OrderInfo) info.clone();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                list.add(tmp);
            } else {
                // 产品表数据
                try {
                    orderInfo = (OrderInfo) info.clone();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // 经过上面的操作，就把订单与产品完全分离出来了，订单在list集合中，产品在单独的一个对象中
        // 然后可以分别综合设置进去
        for (OrderInfo tmp : list) {
            tmp.setPname(orderInfo.getPname());
            tmp.setCategoryId(orderInfo.getCategoryId());
            tmp.setPrice(orderInfo.getPrice());
            // 最后输出
            context.write(tmp, NullWritable.get());
        }
    }
}
