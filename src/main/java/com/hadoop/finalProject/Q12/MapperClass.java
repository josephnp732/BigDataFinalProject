package com.hadoop.finalProject.Q12;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text timeZone = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");
        if (!tokens[0].equals("ID")) {
            if(tokens[20].startsWith("US")) {
                String side = tokens[20];

                timeZone.set(side);
                context.write(timeZone, one);
            }
        }
    }

}
