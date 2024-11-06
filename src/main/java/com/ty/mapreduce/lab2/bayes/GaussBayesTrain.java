package com.ty.mapreduce.lab2.bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GaussBayesTrain {
    private static final long[] count = new long[2];
    private static final double[][] means = new double[2][20]; // 每个特征值的均值
    private static final double[][] variance = new double[2][20]; // 每个特征值的方差

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(BayesTrain.class);
        job.setMapperClass(GaussBayesTrainMapper.class);
        job.setReducerClass(GaussBayesTrainReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\learn\\大数据分析\\lab2\\训练数据.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\learn\\大数据分析\\lab2\\output\\训练结果(Gauss)"));
        System.out.println(job.waitForCompletion(true) ? "成功" : "失败");
    }

    private static class GaussBayesTrainMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            int label = Integer.parseInt(tokens[20]);
            count[label]++;
            for (int i = 0; i < 20; i++) {
                double val = Double.parseDouble((tokens[i]));
                means[label][i] += val;
                context.write(new Text(label + "-" + i), new Text(val + ""));
            }
        }
    }

    private static class GaussBayesTrainReducer extends Reducer<Text, Text, Text, NullWritable> {
        private static final double[] prior = new double[2];

        static { // 先验概率
            prior[0] = (double) count[0] / (count[0] + count[1]);
            prior[1] = (double) count[1] / (count[0] + count[1]);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 求这一类特征值的方差和平均值
            String[] tokens = key.toString().split("\t");
            int category = Integer.parseInt(tokens[0].split("-")[0]);
            int feature = Integer.parseInt(tokens[0].split("-")[1]);
            means[category][feature] /= count[category]; // 均值
            double average = means[category][feature];

            for (Text value : values)
                variance[category][feature] += Math.pow(Double.parseDouble(value.toString()) - average, 2);

            variance[category][feature] /= count[category]; // 方差
            String Means = String.format("%.15f", means[category][feature]);
            String Variance = String.format("%.15f", variance[category][feature]);
            String output = category + "-" + feature + "\t\t" + Means + "\t" + Variance;
            context.write(new Text(output), NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("prior\t" + prior[0] + "\t" + prior[1]), NullWritable.get());
        }
    }
}
