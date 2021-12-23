package com.bigdata.hadoop.reduce.mapper;

import com.bigdata.entity.GroupSortModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @ClassName GroupSort
 * @Description:
 * @Author levlin
 * @Date 2021/12/23
 **/
public class GroupSort extends Configured implements Tool {

    public static class GroupSortMapper extends Mapper<LongWritable, Text, GroupSortModel, IntWritable> {
        private static final GroupSortModel groupSortModel = new GroupSortModel();
        private static final IntWritable num = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            if (split != null && split.length >= 2) {
                groupSortModel.set(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
                num.set(Integer.parseInt(split[1]));
                // {"name":40,"num":20} 20
                // System.out.println("mapper输出：" +
                // JsonUtil.toJSON(groupSortModel) + " " + num);
                context.write(groupSortModel, num);
            }
        }
    }

    public static class GroupSortPartitioner extends Partitioner<GroupSortModel, IntWritable> {
        @Override
        public int getPartition(GroupSortModel key, IntWritable value, int numPartitions) {
            return Math.abs(key.getName() * 127) % numPartitions;
        }
    }

    public static class GroupSortComparator extends WritableComparator {
        public GroupSortComparator() {
            super(GroupSortModel.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            GroupSortModel model = (GroupSortModel) a;
            int num = model.getNum();
            GroupSortModel model2 = (GroupSortModel) b;
            int num2 = model2.getNum();
            // comparator输出：20 1
            // System.out.println("comparator输出：" + model.getName() + " " +
            // model.getNum());
            // comparator2输出：20 10
            // System.out.println("comparator2输出：" + model2.getName() + " " +
            // model2.getNum());
            return num == num2 ? 0 : (num < num2 ? -1 : 1);
        }
    }

    public static class GroupSortReduce extends Reducer<GroupSortModel, IntWritable, Text, IntWritable> {
        private static final Text name = new Text();

        @Override
        protected void reduce(GroupSortModel key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            name.set(key + "");
            for (IntWritable value : values) {
                // reduce输出：20 1 1
                System.out.println("reduce输出：" + key + " " + value);
                context.write(name, value);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();

        // 如果目标文件存在则删除
        Path outPath = new Path(strings[1]);
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            boolean flag = fs.delete(outPath, true);
        }

        // 新建一个Job
        Job job = Job.getInstance(conf, "groupSort");
        // 设置jar信息
        job.setJarByClass(GroupSort.class);

        // 设置mapper信息
        job.setMapperClass(GroupSort.GroupSortMapper.class);
        // 设置reduce信息
        job.setReducerClass(GroupSort.GroupSortReduce.class);

        // 设置mapper和reduce的输出格式，如果相同则只需设置一个
        job.setOutputKeyClass(GroupSortModel.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置fs文件地址
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        // 运行
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
