package com.hadoop.finalProject.Q10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, WritableClass> {

    Text cityState = new Text();
    WritableClass wS = new WritableClass();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String []tokens = value.toString().split(",");

        if(tokens[29].isEmpty() || tokens[15].isEmpty() || tokens[17].isEmpty() || tokens[29].equals(" ") || tokens[15].equals(" ") || tokens[17].equals(" ") || tokens[0].equals("ID")) {
            return;
        }

        String state = tokens[17];
        Double windSpeed = Double.parseDouble(tokens[29]);
        String city = tokens[15];

        cityState.set(city + "," + state);

        wS.setAverageWindSpeed(windSpeed);
        wS.setTotalCount(1);

        context.write(cityState, wS);

    }
}
