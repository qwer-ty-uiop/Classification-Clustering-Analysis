package com.ty.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyLogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream aOut;
    private FSDataOutputStream other;

    public MyLogRecordWriter(TaskAttemptContext taskAttemptContext) {
//        创建流
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            aOut = fs.create(new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\output\\a.log"));
            other = fs.create(new Path("E:\\360MoveData\\Users\\Ty\\Desktop\\output\\b.log"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String string = text.toString();
        if (string.contains("aa")) {
            aOut.writeBytes(string + "\n");
        } else {
            other.writeBytes(string + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(aOut);
        IOUtils.closeStream(other);
    }
}
