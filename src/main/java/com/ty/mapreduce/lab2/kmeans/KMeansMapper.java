package com.ty.mapreduce.lab2.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private final double[][] centroids = new double[KMeansClusterAnalysis.K][KMeansClusterAnalysis.DIMENSION];

    @Override
    protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream open = fs.open(new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\质心\\part-r-00000"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(open));
        // 读取质心
        Clusters.getCentroids(reader, centroids);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        double[] features = Clusters.parseFeatures(value);
        key.set(Clusters.findNearestCentroid(features, centroids));
        context.write(key, value);
    }
}
