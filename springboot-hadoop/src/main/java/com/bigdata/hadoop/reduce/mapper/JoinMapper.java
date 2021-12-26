package com.bigdata.hadoop.reduce.mapper;

import com.hadoop.reduce.model.OrderInfo;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @ClassName JoinMapper
 * @Description:
 * @Author levlin
 * @Date 2021/12/25
 **/
@Slf4j
public class JoinMapper extends Mapper<LongWritable, Text, Text, OrderInfo> {
    private Text text = new Text();
    private OrderInfo orderInfo = new OrderInfo();
    private final static String ORDER_FILE_NAME = "order";
    private final static String PRODUCT_FILE_NAME = "product";
    private final static String ORDER_FLAG = "0";
    private final static String PRODUCT_FLAG = "1";
    private final static String _TITLE = "#";

    @SneakyThrows
    @Override
    public void map(LongWritable key, Text value, Context context){
        String line = new String(value.getBytes(), 0, value.getLength(), "UTF-8");
        log.info("map data key = {}, value = {}, context = {}", key, line, context);

        if (_TITLE.equals(line)) {
            return;
        }

        //获取当前任务的输入切片，这个InputSplit是一个最上层抽象类，可以转换成FileSplit
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        // 得到的是文件名，这里根据文件名来判断是哪一种类型的数据，得到的是order或者product
        String fileName = fileSplit.getPath().getName();

        // 我们这里通过文件名判断是哪种数据
        String pid = "";
        String[] spilt = line.split(",");
        if (fileName.startsWith(ORDER_FILE_NAME)) {
            // 加载订单内容，订单数据里面有 订单号，时间，产品ID，数量
            Integer orderId = Integer.parseInt(spilt[0]);
            String orderDate = spilt[1];
            pid = spilt[2];
            Integer amount = Integer.parseInt(spilt[3]);
            orderInfo.set(orderId, orderDate, pid, amount, "", 0, 0.0, ORDER_FLAG);
        } else {
            // 加载产品内容，产品数据有 产品编号，产品名称，种类，价格
            pid = spilt[0];
            String pname = spilt[1];
            Integer categoryId = Integer.parseInt(spilt[2]);
            Double price = Double.valueOf(spilt[3]);
            orderInfo.set(0, "", pid, 0, pname, categoryId, price, PRODUCT_FLAG);
        }
        text.set(pid);
        context.write(text, orderInfo);
    }
}
