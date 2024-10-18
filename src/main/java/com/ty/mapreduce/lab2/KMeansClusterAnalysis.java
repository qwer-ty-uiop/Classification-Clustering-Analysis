package com.ty.mapreduce.lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KMeansClusterAnalysis {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Path centroidsPath = new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\质心\\part-r-00000");
        Path input = new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\聚类数据.txt");
        Path output = new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\output");
        int maxIterations = 10;
        for (int i = 0; i < maxIterations; i++) {
            Job job = Job.getInstance(conf, "" + i);
            job.setJarByClass(KMeansClusterAnalysis.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            job.addCacheFile(centroidsPath.toUri());  // 将质心文件放入分布式缓存

            System.out.println("i" + (job.waitForCompletion(true) ? "成功" : "失败"));

            // 更新质心文件
            updateCentroids(output, centroidsPath, conf);
        }
    }

    private static void updateCentroids(Path outputPath, Path centroidsPath, Configuration conf) throws IOException {
        // 读取新质心并写回HDFS
        FileSystem fs = FileSystem.get(conf);

        Path resultFile = new Path(outputPath, "part-r-00000");
        FSDataInputStream in = fs.open(resultFile);
        FSDataOutputStream out = fs.create(centroidsPath, true); // 覆写

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

        String line;
        while ((line = reader.readLine()) != null) {
            writer.write(line + "\n");
        }
        reader.close();
        writer.close();
        fs.delete(outputPath, true);
    }

    /**
     * mapper
     * 给每个点计算到当前距离的质心
     */
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private List<Point> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path centroidPath = new Path(cacheFiles[0].toString());
            BufferedReader reader = new BufferedReader(new FileReader(centroidPath.toString()));
            String line;
            while ((line = reader.readLine()) != null) {
                centroids.add(new Point(line));
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Point point = new Point(value.toString());
            int nearestCentroid = findNearestCentroid(point);
            context.write(new IntWritable(nearestCentroid), value);
        }

        private int findNearestCentroid(Point point) {
            double minDistance = Double.MAX_VALUE;
            int nearestCentroid = -1;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = point.distanceTo(centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCentroid = i;
                }
            }
            return nearestCentroid;
        }
    }

    /**
     * reducer
     * 重新分配质心
     */
    public static class KMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Point> points = new ArrayList<>();
            for (Text value : values) {
                points.add(new Point(value.toString()));
            }
            Point newCentroid = calculateCentroid(points);
            context.write(NullWritable.get(), new Text(newCentroid.toString()));
        }

        private Point calculateCentroid(List<Point> points) {
            int dimensions = Point.DIMENSIONS;
            double[] sum = new double[dimensions];
            // 计算向量和
            for (int i = 0; i < dimensions; i++)
                for (Point point : points)
                    sum[i] += point.getVector().get(i);
            List<Double> centroidVector = new ArrayList<>();
            // 计算平均值，作为新的质心
            for (int i = 0; i < dimensions; i++) {
                centroidVector.add(sum[i] / points.size());
            }
            return new Point(centroidVector);
        }
    }
}
