package com.ty.mapreduce.writableComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义类实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参构造
 * 4.重写toString方法
 */

public class FlowBean implements WritableComparable<FlowBean> {


    private long upFlow;
    private long downFlow;
    private long totalFlow;

    @Override
    public int compareTo(FlowBean o) {
        return o.totalFlow == this.totalFlow ? Long.compare(this.upFlow, o.upFlow) : Long.compare(o.totalFlow, this.totalFlow);
    }

    //    空参构造
    public FlowBean() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(totalFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        totalFlow = dataInput.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow() {
        totalFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }
}
