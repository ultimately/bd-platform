package com.bigdata.hadoop.reduce.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName WordCountReduce
 * @Description:
 * @Author levlin
 * @Date 2021/12/20
 **/
@Slf4j
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    private String text = "吕";
    private int textSum = 0;
    private List<String> textList = null;

    public WordCountReduce() {
        textList = new ArrayList<>();
        textList.add(text);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);

        String keyStr = key.toString();
        // 未使用分词器，需要根据map传过来的行内容检索并累加
         boolean isHas = keyStr.contains(text);
         if (isHas) {
         textSum++;
         log.info("统计文字：{}，统计次数：{}", text, textSum);
         }

        // 使用分词器，内容已经被统计好了，直接输出即可
        if (textList.contains(keyStr)) {
            log.info("统计文字分词：{}，统计分词次数：{}", keyStr, sum);
        }
    }
}
