package com.ty.mapreduce.lab2;

import java.util.ArrayList;
import java.util.List;

public class Point {
    private List<Double> vector = new ArrayList<Double>();
    public static final int DIMENSIONS = 20;

    public Point(String point) {
        String[] splits = point.split(",");
        for (String s : splits) {
            vector.add(Double.parseDouble(s.trim()));
        }
    }

    public Point(List<Double> vector) {
        this.vector = vector;
    }

    public double distanceTo(Point point) {
        double distance = 0;
        for (int i = 0; i < vector.size(); i++) {
            distance += Math.pow(vector.get(i) - point.vector.get(i), 2);
        }
        return Math.sqrt(distance);
    }

    public List<Double> getVector() {
        return vector;
    }

    public void setVector(List<Double> vector) {
        this.vector = vector;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Double d : vector) {
            builder.append(d).append(",");
        }
        return builder.toString();
    }
}
