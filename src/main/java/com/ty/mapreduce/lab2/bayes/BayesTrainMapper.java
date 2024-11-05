package com.ty.mapreduce.lab2.bayes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BayesTrainMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int[] count = new int[2];

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String label = tokens[20];
        for (int i = 0; i < 20; i++) {
            context.write(new Text(label + "_feature" + i), new Text(tokens[i]));
        }
    }
}
