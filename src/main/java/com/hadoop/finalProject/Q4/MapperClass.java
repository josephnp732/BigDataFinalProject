package com.hadoop.finalProject.Q4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text sideOfRoad = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

       String []tokens =  value.toString().split(",");
       if(!tokens[0].equals("ID")) {
          String side = tokens[14];
          if(!side.equals(" ")) {

              if(side.equals("L")) {
                  side = "Left";
              } else {
                  side = "Right";
              }

              sideOfRoad.set(side);
              context.write(sideOfRoad, one);
          }
       }

       return;

    }
}
