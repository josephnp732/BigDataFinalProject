package com.hadoop.finalProject.Q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text month = new Text();
    IntWritable one = new IntWritable(1);

    SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat format = new SimpleDateFormat("MMMMM");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

       String []tokens =  value.toString().split(",");
       Date date = new Date();

       if(!tokens[0].equals("ID")) {
           String timeStamp = tokens[4];

           try {
               date = parser.parse(timeStamp);
           } catch (ParseException e) {
               e.printStackTrace();
           }
       }

       month.set(format.format(date));

       context.write(month, one);

    }
}
