package com.ty.mapreduce.lab2.bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;

public class BayesPredict {
    private static final double[][][] prob = new double[2][20][3]; // 条件概率
    private static final double[] prior = new double[2]; // 先验概率

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // 获取训练数据
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream open = fs.open(new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\output\\训练结果\\part-r-00000"));
        BufferedReader in = new BufferedReader(new InputStreamReader(open));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("prior")) {
                String label = line.split("\t")[0].split("-")[1];
                prior[Integer.parseInt(label)] = Double.parseDouble(line.split("\t")[1]);
            } else {
                String[] labels = line.split("\t")[0].split("-");
                double probability = Double.parseDouble((line.split("\t")[1]));
                int i = Integer.parseInt(labels[0]), j = Integer.parseInt(labels[1]), k = Integer.parseInt(labels[2]);
                prob[i][j][k] = probability;
            }
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(BayesTrain.class);
        job.setMapperClass(BayesPredictMapper.class);
        job.setReducerClass(BayesPredictReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\测试数据.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\output\\测试结果"));
        System.out.println(job.waitForCompletion(true) ? "成功" : "失败");
    }

    // 分割数据，用于计算其是各个种类的概率，取概率最大的种类作为其种类
    private static class BayesPredictMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            // 特征值
            String[] features = value.toString().split(",");
            double[] probability = new double[2];
            Arrays.fill(probability, 1);
            for (int i = 0; i < 2; i++) {
                // 算P(Y=y|X=x)
                probability[i] = prior[i];
                for (int j = 0; j < features.length; j++) {
                    double val = Double.parseDouble(features[j]);
                    if (val < 0) {
                        probability[i] *= prob[i][j][0];
                    } else if (val <= 1) {
                        probability[i] *= prob[i][j][1];
                    } else {
                        probability[i] *= prob[i][j][2];
                    }
                }
            }
            String label = probability[0] > probability[1] ? "0" : "1";
            context.write(key, new Text(label));
        }
    }

    private static class BayesPredictReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            for (Text value : values)
                context.write(new Text(value.toString()), NullWritable.get());
        }
    }
}
