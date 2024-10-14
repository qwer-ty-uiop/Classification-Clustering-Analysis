package com.ty.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private final FlowBean flowBean = new FlowBean();
    private final Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        phone.set(split[1]);
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];
        flowBean.setUpFlow(Long.parseLong(upFlow));
        flowBean.setDownFlow(Long.parseLong(downFlow));
        flowBean.setTotalFlow();
        context.write(phone, flowBean);
    }
}
