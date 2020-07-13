package com.hadoop.finalProject.Q9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<CompositeKeyWritable, IntWritable, Text, IntWritable> {

    Text text = new Text();
    IntWritable count = new IntWritable();
    StateMap stateMap = new StateMap();

    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for(IntWritable v : values) {
            sum += v.get();
        }

        text.set(stateMap.getStateFromAbbr(key.getState())+ " - in Year - " + key.getYear() + " : ");
        count.set(sum);

        context.write(text, count);

    }
}
