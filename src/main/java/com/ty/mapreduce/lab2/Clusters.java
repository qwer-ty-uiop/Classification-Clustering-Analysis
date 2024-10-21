package com.ty.mapreduce.lab2;

import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;

public class Clusters {
    /**
     * 获取质心
     *
     * @param reader    质心文件的reader
     * @param centroids 用于存质心各个纬度数据的数组
     */
    public static void getCentroids(BufferedReader reader, double[][] centroids) throws IOException {
        for (int i = 0; i < KMeansClusterAnalysis.K; i++) {
            String[] line = reader.readLine().split(",");
            for (int j = 0; j < KMeansClusterAnalysis.DIMENSION; j++) {
                centroids[i][j] = Double.parseDouble(line[j]);
            }
        }
    }

    /**
     * 找最近的质心
     *
     * @param features  当前结点
     * @param centroids 质心集合
     * @return 最近质心的编号
     */
    public static int findNearestCentroid(double[] features, double[][] centroids) {
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

    /**
     * 将向量字符串转化为向量
     *
     * @param value 向量字符串
     * @return 向量数组
     */
    public static double[] parseFeatures(Text value) {
        String[] featureStr = value.toString().split(",");
        double[] features = new double[featureStr.length];
        for (int i = 0; i < featureStr.length; i++) {
            features[i] = Double.parseDouble(featureStr[i]);
        }
        return features;
    }
}
