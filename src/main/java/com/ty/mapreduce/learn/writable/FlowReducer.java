package com.ty.mapreduce.learn.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private final FlowBean flow = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long sumUp = 0,sumDown=0;
        for(FlowBean flowBean : values) {
            sumUp+=flowBean.getUpFlow();
            sumDown+=flowBean.getDownFlow();
        }
        flow.setUpFlow(sumUp);
        flow.setDownFlow(sumDown);
        flow.setTotalFlow();
        context.write(key, flow);
    }
}
