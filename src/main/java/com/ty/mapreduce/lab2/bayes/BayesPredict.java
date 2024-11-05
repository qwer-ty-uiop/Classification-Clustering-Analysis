package com.ty.mapreduce.lab2.bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

public class BayesPredict {
    private static final String[][][] prob = new String[2][20][3];

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // 获取训练数据
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream open = fs.open(new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\训练结果\\part-r-00000"));
        BufferedReader in = new BufferedReader(new InputStreamReader(open));
        String line;
        while ((line = in.readLine()) != null) {
            String[] labels = line.split("\t")[0].split("-");
            String probability = line.split("\t")[1];
            int i = Integer.parseInt(labels[0]), j = Integer.parseInt(labels[1]), k = Integer.parseInt(labels[2]);
            prob[i][j][k] = probability;
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(BayesTrain.class);
        job.setMapperClass(BayesPredictMapper.class);
        job.setReducerClass(BayesPredictReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\训练数据.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\训练结果"));
        System.out.println(job.waitForCompletion(true) ? "成功" : "失败");
    }

    private static class BayesPredictMapper extends Mapper<LongWritable, Text, Text, Text> {

    }
}
