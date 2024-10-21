package com.ty.mapreduce.lab2;

import org.apache.hadoop.conf.Configuration;
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

public class ClusteringClassify {

    private static Path centroidsPath;

    public static void classifyData(Path input, Path output, Path centroids) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.job.reduces", false);
        Job job = Job.getInstance(conf);
        job.setJarByClass(ClusteringClassify.class);
        job.setMapperClass(ClusteringClassifyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        centroidsPath = centroids;
        System.out.println(job.waitForCompletion(true) ? " 成功" : " 失败");
    }

    private static class ClusteringClassifyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        double[][] centroids = new double[KMeansClusterAnalysis.K][KMeansClusterAnalysis.DIMENSION];

        @Override
        protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(centroidsPath)));
            Clusters.getCentroids(reader, centroids);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            // 初始化向量
            double[] features = Clusters.parseFeatures(value);
            // 将向量分类，并保持顺序不变
            int nearestCentroid = Clusters.findNearestCentroid(features, centroids);
            value.set(nearestCentroid + ", " + value);
            context.write(key, value);
        } // 不需要reduce阶段了
    }
}
