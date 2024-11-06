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
import java.util.function.Predicate;

public class GaussBayesPredict {
    private static final double[] prior = new double[2];
    private static final double[][] means = new double[2][20]; // 每个特征值的均值
    private static final double[][] variance = new double[2][20]; // 每个特征值的方差
    private static long totalNumber = 0;
    private static long consistentNumber = 0;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream open = fs.open(new Path("D:\\learn\\大数据分析\\lab2\\output\\训练结果(Gauss)\\part-r-00000"));
        BufferedReader in = new BufferedReader(new InputStreamReader(open));
        String line;
        // 处理初始化数据
        while ((line = in.readLine()) != null) {
            if (line.startsWith("prior")) {
                prior[0] = Double.parseDouble(line.split("\t")[1]);
                prior[1] = Double.parseDouble(line.split("\t")[2]);
                continue;
            }
            String[] tokens = line.split("\t");
            String[] labels = tokens[0].split("-");
            int category = Integer.parseInt(labels[0]);
            int feature = Integer.parseInt(labels[1]);
            double meansVal = Double.parseDouble(tokens[2]);
            double varianceVal = Double.parseDouble(tokens[3]);
            means[category][feature] = meansVal;
            variance[category][feature] = varianceVal;
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(BayesTrain.class);
        job.setMapperClass(GaussBayesPredictMapper.class);
        job.setReducerClass(GaussBayesPredictReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\learn\\大数据分析\\lab2\\验证数据.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\learn\\大数据分析\\lab2\\output\\验证结果(Gauss)"));
        System.out.println(job.waitForCompletion(true) ? "成功" : "失败");
    }

    private static class GaussBayesPredictMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            totalNumber++;
            // 计算每个类别，找出后验概率最大的类别
            double[] probability = new double[2];
            String[] features = value.toString().split(",");
            for (int i = 0; i < 2; i++) {
                probability[i] = Math.log(prior[i]);
                for (int j = 0; j < 20; j++) {
                    double x = Double.parseDouble(features[j]);
                    probability[i] += predict(x, variance[i][j], means[i][j]);
                }
            }
            String label = probability[0] > probability[1] ? "0" : "1";
            context.write(key, new Text(label));
            consistentNumber += label.equals(features[20]) ? 1 : 0;
        }
    }

    private static class GaussBayesPredictReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            for (Text value : values)
                context.write(value, NullWritable.get());
        }

        @Override
        protected void cleanup(Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            context.write(new Text("accuracy=" + (double) consistentNumber / totalNumber), NullWritable.get());
        }
    }


    /**
     * 求概率密度函数(似然性)，通过对数化简计算，并舍弃一些常数值，最终用log(P(X|Ck)) + log(P(Ck))来近似等效P(Ck|X)
     * 似然函数的一部分
     *
     * @param x        特征值
     * @param variance 方差
     * @param means    均值
     * @return
     */
    private static double predict(double x, double variance, double means) {
        return (Math.log(variance) + Math.pow(x - means, 2) / variance) * -0.5;
    }

}
