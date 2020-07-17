package com.hadoop.finalProject.Q8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<Object, Text, IntWritable, TempWritable> {

    IntWritable sev = new IntWritable();
    TempWritable tempWritable = new TempWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        if (!tokens[0].equals("ID")) {

            if(tokens[3].isEmpty() || tokens[23].isEmpty() || tokens[3].equals(" ") || tokens[26].equals(" ")) {
                return;
            }

            int severity = Integer.parseInt(tokens[3]);
            double temperature = Double.parseDouble(tokens[23]);

            tempWritable.setAverageTemp(temperature);
            tempWritable.setMaxTemp(temperature);
            tempWritable.setMinTemp(temperature);
            tempWritable.setCount(1);

            sev.set(severity);

            context.write(sev, tempWritable);
        }
    }
}
