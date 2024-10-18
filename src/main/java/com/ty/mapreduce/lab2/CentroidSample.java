package com.ty.mapreduce.lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CentroidSample {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CentroidSample.class);
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\聚类数据.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\质心"));

        System.out.println(job.waitForCompletion(true) ? "成功" : "失败");

    }

    // 质心数量
    private static final int K = 5;

    // mapper
    private static class SampleMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        Random random = new Random();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (random.nextDouble() < 0.02)
                context.write(new IntWritable(random.nextInt(K)), value);
        }
    }

    // reducer
    private static class SampleReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        private List<String> centroids = new ArrayList<String>();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                centroids.add(value.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Random random = new Random();
            for (int i = 1; i <= K; i++) {
                String centroid = centroids.get(random.nextInt(centroids.size()));
                context.write(NullWritable.get(), new Text(centroid));
            }
        }
    }
}
