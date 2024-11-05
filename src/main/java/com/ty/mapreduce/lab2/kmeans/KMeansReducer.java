package com.ty.mapreduce.lab2.kmeans;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] newCentroid = new double[KMeansClusterAnalysis.DIMENSION];
        int clusteringNum = 0;
        for (Text value : values) {
            String[] features = value.toString().split(",");
            for (int i = 0; i < KMeansClusterAnalysis.DIMENSION; i++) {
                newCentroid[i] += Double.parseDouble(features[i]);
            }
            clusteringNum++;
        }
        for (int i = 0; i < KMeansClusterAnalysis.DIMENSION; i++) {
            newCentroid[i] /= clusteringNum;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < KMeansClusterAnalysis.DIMENSION; i++) {
            sb.append(newCentroid[i]).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        // 加入新的质心
        context.write(key, new Text(sb.toString()));
    }
}
