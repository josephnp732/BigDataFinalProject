package com.hadoop.finalProject.Q10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MainMapperClass extends Mapper<LongWritable, Text, Text, Text> {

    Text outKey = new Text();
    Text outValue = new Text();
    boolean first = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String tokens[] = value.toString().split(",");

        if(tokens[17].equals(" ") || tokens[17].isEmpty() || tokens[0].equals("ID")) {
            return;
        }

        String stateAbbr = tokens[17];

        StringBuilder sB = new StringBuilder();

        sB.append("@");

        for(String token : tokens) {
            if(first) {
                first = false;
            } else {
                sB.append(",");
            }
            sB.append(token);
        }

        outKey.set(stateAbbr);
        outValue.set(sB.toString());

        context.write(outKey, outValue);
    }
}
