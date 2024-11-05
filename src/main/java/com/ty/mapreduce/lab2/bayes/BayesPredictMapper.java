package com.ty.mapreduce.lab2.bayes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BayesPredictMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String,Double>priors = new HashMap<>();         // 先验概率
    private Map<String, Double[]> means = new HashMap<>();      // 均值
    private Map<String, Double[]> variances = new HashMap<>();  // 方差

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

    }
}
