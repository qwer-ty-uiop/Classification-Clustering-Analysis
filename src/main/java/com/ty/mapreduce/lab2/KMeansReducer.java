package com.ty.mapreduce.lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double[] newCentroid = new double[KMeansClusterAnalysis.DIMENSION];
        int clusteringNum = 0;
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            for (int i = 0; i < KMeansClusterAnalysis.DIMENSION; i++) {
                newCentroid[i] += Double.parseDouble(tokens[i]);
            }
            clusteringNum++;
        }
        for (int i = 0; i < KMeansClusterAnalysis.DIMENSION; i++) {
            newCentroid[i] /= clusteringNum;
        }
        // 输出新的质心
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < KMeansClusterAnalysis.DIMENSION; i++) {
            sb.append(String.format("%.2f", newCentroid[i]));
            if (i + 1 < KMeansClusterAnalysis.DIMENSION)
                sb.append(",");
        }
        context.write(key, new Text(sb.toString()));
    }
}
