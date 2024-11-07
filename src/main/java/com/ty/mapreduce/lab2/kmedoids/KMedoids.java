package com.ty.mapreduce.lab2.kmedoids;

import com.ty.mapreduce.lab2.utils.ClusteringClassify;
import com.ty.mapreduce.lab2.utils.Clusters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.ty.mapreduce.lab2.utils.Clusters.copyToCentroidFile;

public class KMedoids {

    public static final int maxIterations = 2;
    public static final int K = 3;
    public static final int DIMENSION = 20;

    private static final Path centroidsPath = new Path("D:\\learn\\大数据分析\\lab2\\output\\质心(KMedoids)\\part-r-00000");
    private static final Path input = new Path("D:\\learn\\大数据分析\\lab2\\聚类数据.txt");
    private static final Path output = new Path("D:\\learn\\大数据分析\\lab2\\output\\聚类结果(KMedoids)");

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // 处理输出文件
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        if (fs.exists(centroidsPath.getParent())) {
            fs.delete(centroidsPath.getParent(), true);
        }
        CentroidInitializer.InitializeCentroid();
        for (int i = 0; i < maxIterations; i++) {
            Job job = Job.getInstance(conf, "jobName=" + i);
            job.setJarByClass(KMedoids.class);
            job.setMapperClass(KMedoidsMapper.class);
            job.setReducerClass(KMedoidsReducer.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job, input);
            FileOutputFormat.setOutputPath(job, output);
            System.out.println(job.waitForCompletion(true) ? "成功" : "失败");

            copyToCentroidFile(fs, output, centroidsPath);
            // 删除输出路径
            fs.delete(output, true);
        }
        // 根据质心进行聚类分析
        ClusteringClassify.classifyData(input, output, centroidsPath);
    }

    private static class KMedoidsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        double[][] centroids = new double[K][DIMENSION];
        private static final Random random = new Random();

        @Override
        protected void setup(Context context) throws IOException {
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(centroidsPath)));
            Clusters.getCentroids(reader, centroids);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double[] features = Clusters.parseFeatures(value);
            // kmedoids的复杂度很高，执行速度太慢了，通过随机抽样来重新确定质心
            key.set(Clusters.findNearestCentroid(features, centroids)); // 聚类
            if (random.nextDouble() < 0.005)
                context.write(key, value);
        }
    }

    private static class KMedoidsReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int clusteringNum = 0;
            List<String> nodes = new ArrayList<>();
            for (Text value : values) {
                nodes.add(value.toString()); // 记录每个类的结点
                clusteringNum++;
            }

            // 计算每个节点到其他所有节点距离和，取最小的为中心结点
            String centroid = nodes.get(0);
            double minDistance = 1e9 + 7;
            for (int i = 0; i < clusteringNum; i++) {
                String[] curFeatures = nodes.get(i).split(",");
                double curDistance = 0;
                // 计算每个节点到其他所有节点距离和(曼哈顿距离)
                for (int j = 0; j < clusteringNum; j++) {
                    if (j == i) continue;
                    String[] features = nodes.get(j).split(",");
                    for (int k = 0; k < DIMENSION; k++) {
                        curDistance += Double.parseDouble(curFeatures[k]) - Double.parseDouble(features[k]);
                    }
                }
                if (curDistance < minDistance) {
                    minDistance = curDistance;
                    centroid = nodes.get(i);
                }
            }
            // 加入新的质心
            context.write(key, new Text(centroid));
        }
    }

}
