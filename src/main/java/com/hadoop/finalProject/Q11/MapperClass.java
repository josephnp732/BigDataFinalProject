package com.hadoop.finalProject.Q11;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text proximity = new Text();
    IntWritable one = new IntWritable(1);

    RowHeaderClass rhc = new RowHeaderClass();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");
        if (!tokens[0].equals("ID")) {

            for(int i = 32; i < 45; i++) {
                if(tokens[i].equals("True")) {
                    proximity.set(rhc.getRowName(i));
                    context.write(proximity, one);
                }
            }
        }
    }

}
