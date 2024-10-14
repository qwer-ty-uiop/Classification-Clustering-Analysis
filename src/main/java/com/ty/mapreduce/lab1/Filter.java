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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class Filter {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Filter.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Location.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // FileInputFormat.setInputPaths(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileInputFormat.setInputPaths(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\D_Sample\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\D_Filter"));

        System.out.println("执行结果: " + (job.waitForCompletion(true) ? "成功" : "失败"));

    }

    public static class FilterMapper extends Mapper<LongWritable, Text, Location, Text> {
        // rating 范围 [min, max]
        private static final double min = 0, max = 100;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Location location = new Location();
            String[] splits = value.toString().split("\\|");
            int removeLength = splits[splits.length - 2].length() + 1;

            // 处理属性格式
            splits[4] = changeDateFormat(splits[4]); // review_date
            splits[8] = changeDateFormat(splits[8]); // user_birthday
            if (splits[5].endsWith("℉")) splits[5] = splits[5].replace("℉", "℃"); // temperature
            // rating 归一化
            if (!"?".equals(splits[6])) {
                double rating = Double.parseDouble(splits[6]);
                rating = (rating - min) / (max - min);
                splits[6] = String.format("%.2f", rating);
            }
            // 设置输出主键，在shuffle阶段对longitude和latitude排序
            location.setLongitude(Double.parseDouble(splits[1]));
            location.setLatitude(Double.parseDouble(splits[2]));
            // 设置输出值
            StringBuilder sb = new StringBuilder();
            for (String split : splits)
                sb.append(split).append("|");

            Text outputValue = new Text(sb.toString());
            context.write(location, outputValue);
        }

        // 目标数据格式
        private static final DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        // 数据格式集合
        private static final DateTimeFormatter[] formatters = new DateTimeFormatter[]{
                DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                DateTimeFormatter.ofPattern("yyyy/MM/dd"),
                DateTimeFormatter.ofPattern("MMMM d,yyyy", Locale.ENGLISH)
        };

        /**
         * @param dateString 需要转换格式的日期
         * @return 转换格式后的日期
         */
        private String changeDateFormat(String dateString) {
            LocalDate date = null;
            for (DateTimeFormatter formatter : formatters) {
                try {
                    date = LocalDate.parse(dateString, formatter);
                    break;
                } catch (Exception e) {
                    // 忽略转换失败的格式
                }
            }
            if (date != null) return date.format(targetFormatter);
            else throw new IllegalArgumentException("Unsupported date format: " + dateString);
        }
    }

    public static class FilterReducer extends Reducer<Location, Text, Text, NullWritable> {
        @Override
        protected void reduce(Location key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                double longitude = key.getLongitude();
                double latitude = key.getLatitude();
                if (longitude >= 8.1461259 && longitude <= 11.1993265 && latitude >= 56.5824856 && latitude <= 57.750511)
                    context.write(value, NullWritable.get());
            }
        }
    }


}
