package com.ty.mapreduce.learn.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
//        1.获取job
        Job instance = Job.getInstance(new Configuration());
//        2.获取jar包路径
        instance.setJarByClass(WordCountDriver.class);
//        3.关联mapper和reducer
        instance.setMapperClass(WordCountMapper.class);
        instance.setReducerClass(WordCountReducer.class);
//        4.设置mapper的输出的key value类型
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);
//        5.设置最终输出的key value类型
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);
//        6.设置输入/输出路径
        FileInputFormat.setInputPaths(instance, new Path(args[0]));
        FileOutputFormat.setOutputPath(instance, new Path(args[1]));
//        7.提交job
        boolean b = instance.waitForCompletion(true);// 参数：是否监控并打印job的信息
        System.exit(b ? 0 : 1);
    }
}
