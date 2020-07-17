package com.hadoop.finalProject.Q6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

    Text state = new Text();
    Text cities = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

       String []tokens =  value.toString().split(",");

       String cityString = "";
       String stateString = "";

       if(!tokens[0].equals("ID")) {
           cityString = tokens[15];
           stateString = tokens[17];
       }

       state.set(stateString);
       cities.set(cityString);

       context.write(state, cities);

    }
}
