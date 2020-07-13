package com.hadoop.finalProject.Q5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text state = new Text();
    IntWritable one = new IntWritable(1);

    StateMap statesMap = new StateMap();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

       String []tokens =  value.toString().split(",");

       String stateString = "";

       if(!tokens[0].equals("ID")) {
           stateString = statesMap.getStateFromAbbr(tokens[17]);
       }

       state.set(stateString);


       context.write(state, one);

    }
}
