package com.hadoop.finalProject.Q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<Text, WritableClass> {

    @Override
    public int getPartition(Text key, WritableClass writableClass, int i) {
        return key.toString().split(",")[1].hashCode() % i;
    }
}
