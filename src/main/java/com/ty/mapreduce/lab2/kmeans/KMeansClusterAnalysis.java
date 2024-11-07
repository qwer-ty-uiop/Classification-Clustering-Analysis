package com.ty.mapreduce.lab2.kmeans;

import com.ty.mapreduce.lab2.utils.ClusteringClassify;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

class KMeansClusterAnalysis {

    public static final int maxIterations = 2;
    public static final int K = 3;
    public static final int DIMENSION = 20;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Path centroidsPath = new Path("D:\\learn\\大数据分析\\lab2\\output\\质心\\part-r-00000");
        Path input = new Path("D:\\learn\\大数据分析\\lab2\\聚类数据.txt");
        Path output = new Path("D:\\learn\\大数据分析\\lab2\\output\\聚类结果");
        // 处理输出文件
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        if (fs.exists(centroidsPath.getParent())) {
            fs.delete(centroidsPath.getParent(), true);
        }

        // 初始化质心文件
        CentroidInitializer.InitializeCentroid();

        for (int i = 1; i <= maxIterations; i++) {
            Job job = Job.getInstance(conf, "" + i);
            job.setJarByClass(KMeansClusterAnalysis.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            System.out.println(i + (job.waitForCompletion(true) ? " 成功" : " 失败"));
            // 删除输出路径
            fs.delete(output, true);

        }

        ClusteringClassify.classifyData(input, output, centroidsPath);

    }
}