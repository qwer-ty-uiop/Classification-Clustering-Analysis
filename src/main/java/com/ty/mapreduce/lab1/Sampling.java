package com.ty.mapreduce.lab1;

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
import java.util.*;


public class Sampling {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Sampling.class);
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        FileInputFormat.setInputPaths(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\data.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\D_Sample"));

        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class SampleMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();
        private final Random random = new Random();
        private final Map<String, List<String>> reservoirMap = new HashMap<>();
        private final Map<String, Integer> categoryCounts = new HashMap<>();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\|");
            String category = splits[splits.length - 2];
            // 获取当前类别的蓄水池
            List<String> reservoir = reservoirMap.getOrDefault(category, new ArrayList<String>());
            categoryCounts.merge(category, 1, Integer::sum);
            // 蓄水池没满，就直接加入
            // 设置每个种类每个Mapper的蓄水池容量：每个种类的总样本数 = sampleSize * MapperNumber
            int sampleSize = 100;
            if (reservoir.size() < sampleSize) {
                reservoir.add(value.toString());
            } else {
                int randomIndex = random.nextInt(categoryCounts.get(category) + 1);
                if (randomIndex < sampleSize) {
                    reservoir.set(randomIndex, value.toString());
                }
            }
            //更新蓄水池
            reservoirMap.put(category, reservoir);
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, List<String>> entry : reservoirMap.entrySet()) {
                String category = entry.getKey();
                for (String sample : entry.getValue()) {
                    outputKey.set(category);
                    outputValue.set(sample);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class SampleReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(null, value);
            }
        }
    }
}
