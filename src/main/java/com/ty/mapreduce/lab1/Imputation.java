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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Imputation {
    // 国家->职业->工资
    private static final Map<String, Map<String, List<Double>>> nationAndCareerToIncome = new ConcurrentHashMap<>();


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Imputation.class);
        job.setMapperClass(imputationMapper.class);
        job.setReducerClass(imputationReducer.class);

        job.setMapOutputKeyClass(Location.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        FileInputFormat.setInputPaths(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\D_Filter"));
//        FileOutputFormat.setOutputPath(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\D_Done"));

        System.out.println("执行结果: " + (job.waitForCompletion(true) ? "成功" : "失败"));
    }

    public static class imputationMapper extends Mapper<LongWritable, Text, Location, Text> {
        private final Location outputKey = new Location();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\|");
            String userIncome = splits[11];
            String userCareer = splits[10];
            String userNation = splits[9];
            String longitude = splits[1];
            String latitude = splits[2];
            String altitude = splits[3];
//            String rating = splits[6];
            // 先处理 userIncome
            Map<String, List<Double>> careerToIncome = nationAndCareerToIncome.getOrDefault(userNation, new ConcurrentHashMap<>());
            List<Double> incomeList = careerToIncome.getOrDefault(userCareer, new ArrayList<>());
            if ("?".equals(userIncome)) {
                // 用绝对众数填充缺失值，摩尔投票方法求众数：时间 O(n),空间 O(1)，此处绝对众数指数量严格大于总数一半的数据
                // 如果没有绝对众数，填充值就是上一次加入List的结果
                // 如果当前国籍职业是第一次加入，那么填充默认值
                int cnt = 0;
                // 默认值 2000
                double mode = 2000.0;
                for (double d : incomeList) {
                    if (cnt == 0)
                        mode = d;
                    // 两数相等，则cnt++
                    if (String.format("%.2f", d).equals(String.format("%.2f", mode)))
                        cnt++;
                    else cnt--;
                }
                // 填充结果
                splits[11] = String.format("%.2f", mode); // userIncome
                // 更新缓存
                incomeList.add(mode);
            } else {
                // 更新缓存
                incomeList.add(Double.parseDouble(userIncome));
            }
            // 已经有数据添加数据到缓存
            careerToIncome.put(userCareer, incomeList);
            nationAndCareerToIncome.put(userNation, careerToIncome);

            // 处理 rating
            // 默认值填充
            // if ("?".equals(splits[6]))
            //     splits[6] = "50.00";

            // 保证排序性
            outputKey.setLatitude(Double.parseDouble(latitude));
            outputKey.setLongitude(Double.parseDouble(longitude));
            StringBuilder sb = new StringBuilder();
            for (String split : splits)
                sb.append(split).append("|");

            Text outputValue = new Text(sb.toString());
            context.write(outputKey, outputValue);
        }
    }

    public static class imputationReducer extends Reducer<Location, Text, NullWritable, Text> {
        @Override
        protected void reduce(Location key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 直接输出即可
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }
}
