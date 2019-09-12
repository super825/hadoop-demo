package com.ada.partition;

import com.ada.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();

        switch (phone.substring(0, 3)) {
            case "135":
                return 0;
            case "136":
                return 1;
            case "137":
                return 2;
            case "138":
                return 3;
            case "139":
                return 4;
            default:
                return 5;
        }
    }
}
