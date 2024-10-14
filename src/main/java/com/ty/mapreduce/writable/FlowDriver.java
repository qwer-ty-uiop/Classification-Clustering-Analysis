package com.ty.mapreduce.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//       1.获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//       2.设置jar,即怎么调用这个jar包
        job.setJarByClass(FlowDriver.class);
//       3.关联mapper、reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
//       4.设置mapper输出 key value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
//       5.设置最终输出的key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
//     （*）设置分区参数
//        job.setPartitionerClass(FlowPartitioner.class);
//        job.setNumReduceTasks(5);

//       6.设置数据输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\output1"));
//       7.提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
