package com.ty.mapreduce.lab2.bayes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BayesTrainReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int cnt = 0;
        double sum = 0.0, sumOfSquares = 0.0;

        for (Text val : values) {
            double feature = Double.parseDouble(val.toString());
            cnt++;
            sum += feature;
            sumOfSquares += feature * feature;
        }
        // 均值、方差
        double mean = sum / cnt;
        double variance = sumOfSquares / cnt;
        context.write(key, new Text("mean=" + mean + "\tvariance=" + variance));
    }
}
