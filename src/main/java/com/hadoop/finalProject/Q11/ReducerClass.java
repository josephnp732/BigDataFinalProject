package com.hadoop.finalProject.Q11;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, IntWritable, Text, Text> {

    Text percentage = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        for(IntWritable v : values) {
            count += v.get();
        }

        int total = 3513617;  // total number of records

        double perc = ((double) count / total) * 100;
        percentage.set("\t" + String.format("%.2f", perc) + "%");

        context.write(key, percentage);
    }
}
