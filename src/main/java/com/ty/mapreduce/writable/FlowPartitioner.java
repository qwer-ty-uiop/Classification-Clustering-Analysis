package com.ty.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text phoneNumber, FlowBean flowBean, int i) {
        String phone = phoneNumber.toString();
        String substring = phone.substring(0, 3);
        switch (substring) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }

}
