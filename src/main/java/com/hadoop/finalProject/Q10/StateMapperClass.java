package com.hadoop.finalProject.Q10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StateMapperClass extends Mapper<LongWritable, Text, Text, Text> {

    Text outKey = new Text();
    Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String tokens[] = value.toString().split(",");

        String state = tokens[1];
        String stateAbbr = tokens[0];

        String prefix = "#";

        outKey.set(stateAbbr);
        outValue.set(prefix + state);

        context.write(outKey, outValue);
    }
}
