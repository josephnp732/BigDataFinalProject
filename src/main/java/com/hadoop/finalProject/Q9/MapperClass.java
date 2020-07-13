package com.hadoop.finalProject.Q9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MapperClass extends Mapper<Object, Text, CompositeKeyWritable, IntWritable> {

    SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat formater = new SimpleDateFormat("yyyy");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String []tokens = value.toString().split(",");

        if(tokens[4].isEmpty() || tokens[17].isEmpty() || tokens[4].equals(" ") || tokens[17].equals(" ") || tokens[0].equals("ID")) {
            return;
        }

        String stateString = tokens[17];
        String year = null;
        try {
            year = formater.format(parser.parse(tokens[4]));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        CompositeKeyWritable cKW = new CompositeKeyWritable(stateString, year);

        context.write(cKW, new IntWritable(1));
    }
}
