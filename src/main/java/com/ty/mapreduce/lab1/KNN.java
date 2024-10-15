package com.ty.mapreduce.lab1;

import java.util.ArrayList;
import java.util.List;

public class KNN {
    // 依赖项集合
    private List<double[]> points;
    // 评分集合
    private List<Double> ratings;
    private int k;

    public KNN(int k) {
        this.k = k;
        points = new ArrayList<>();
        ratings = new ArrayList<>();
    }


    public void addPoint(double[] point, double rating) {
        points.add(point);
        ratings.add(rating);
    }

    /**
     * @param testPoint 元组的属性值数组
     * @return 预测的 rating 值
     */
    public double classify(double[] testPoint) {
        return predictRating(getNode(testPoint));
    }

    // 返回 k 最邻近结点
    private List<Node> getNode(double[] point) {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < points.size(); i++) {
            double distance = distance(point, points.get(i));
            nodes.add(new Node(ratings.get(i), distance));
        }
        return nodes.subList(0, Math.min(nodes.size(), k));
    }

    // 预测值就取 k 最邻近结点的 rating 的平均值
    private double predictRating(List<Node> nodes) {
        double average = 0;
        for (Node node : nodes) {
            average += node.rating;
        }
        return average / nodes.size();
    }

    // 计算欧氏距离
    private double distance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(sum);
    }

    private static class Node {
        double rating;
        double distance;

        public Node(double rating, double distance) {
            this.rating = rating;
            this.distance = distance;
        }

        public double getDistance() {
            return distance;
        }

        public double getRating() {
            return rating;
        }
    }
}
