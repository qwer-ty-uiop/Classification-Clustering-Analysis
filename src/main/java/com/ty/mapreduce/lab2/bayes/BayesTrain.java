package com.ty.mapreduce.lab2.bayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BayesTrain {

    private static final long[] count = new long[2];

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(BayesTrain.class);
        job.setMapperClass(BayesTrainMapper.class);
        job.setReducerClass(BayesTrainReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\训练数据.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\output\\训练结果"));
        System.out.println(job.waitForCompletion(true) ? "成功" : "失败");
    }

    private static class BayesTrainMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            int label = Integer.parseInt(tokens[20]);
            // 统计分类数量
            count[label]++;
            // 将每个类别的每个维度分为三类（x > 1、1 >= x >= 0、0 > x），作为分类依据
            for (int i = 0; i < 20; i++) {
                String labelStr;
                double val = Double.parseDouble((tokens[i]));
                if (val < 0) labelStr = label + "-" + 0 + "-" + i;
                else if (val <= 1) labelStr = label + "-" + 1 + "-" + i;
                else labelStr = label + "-" + 2 + "-" + i;
                // 类别 + 数据
                // 类别形式为(数据类别，维度类别，具体维度)
                context.write(new Text(labelStr), new Text(tokens[i]));
            }
        }
    }

    // 求先验概率、类别为x时特征分类为y条件概率（需要对每一维度分别求）
    private static class BayesTrainReducer extends Reducer<Text, Text, Text, Text> {
        private static final double[][][] condProbability = new double[2][20][3];
        private static final double[] priorProbability = new double[2];

        static {
            priorProbability[0] = (double) count[0] / (count[0] + count[1]); // 先验概率
            priorProbability[1] = (double) count[1] / (count[0] + count[1]); // 先验概率
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) {
            String[] labels = key.toString().split("-");
            int category = Integer.parseInt(labels[0]); // 标签y
            int feature = Integer.parseInt(labels[2]); // 具体维度Xi
            int label = Integer.parseInt(labels[1]); // 维度的分类xi
            // 求P(Xi=xi|Y=y) = numberOfXInThisCategory / count[category]
            int cnt = 0;
            for (Text val : values)
                cnt++;
            condProbability[category][feature][label] = (double) cnt / count[category];
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (int i = 0; i < 2; i++) {
                // 条件概率
                for (int j = 0; j < 20; j++)
                    for (int k = 0; k < 3; k++)
                        context.write(new Text(i + "-" + j + "-" + k), new Text(condProbability[i][j][k] + ""));
                // 先验概率
                context.write(new Text("prior-" + i), new Text(priorProbability[i] + ""));
            }
        }
    }
}
