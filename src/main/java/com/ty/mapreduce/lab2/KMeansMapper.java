package com.ty.mapreduce.lab2;

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
    private double[][] centroids = new double[KMeansClusterAnalysis.K][KMeansClusterAnalysis.DIMENSION];

    @Override
    protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream open = fs.open(new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\质心\\part-r-00000"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(open));
        // 读取质心
        for (int i = 0; i < KMeansClusterAnalysis.K; i++) {
            String[] line = reader.readLine().split(",");
            for (int j = 0; j < KMeansClusterAnalysis.DIMENSION; j++) {
                centroids[i][j] = Double.parseDouble(line[j]);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        String[] featureStr = value.toString().split(",");
        double[] features = new double[KMeansClusterAnalysis.DIMENSION];
        for (int i = 0; i < KMeansClusterAnalysis.DIMENSION; i++) {
            features[i] = Double.parseDouble(featureStr[i]);
        }
        key.set(findNearestCentroid(features));
        context.write(key, value);
    }

    private int findNearestCentroid(double[] features) {
        int nearestCentroid = 0;
        double minDistance = Double.MAX_VALUE;
        for (int i = 0; i < KMeansClusterAnalysis.K; i++) {
            double distance = 0;
            for (int j = 0; j < KMeansClusterAnalysis.DIMENSION; j++) {
                distance += Math.pow(features[j] - centroids[i][j], 2);
            }
            if (distance < minDistance) {
                minDistance = distance;
                nearestCentroid = i;
            }
        }
        return nearestCentroid;
    }
}
