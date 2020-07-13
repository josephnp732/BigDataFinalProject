package com.hadoop.finalProject.Q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinerClass extends Reducer<Text, WritableClass, Text, WritableClass> {

    WritableClass newTuple = new WritableClass();

    @Override
    protected void reduce(Text key, Iterable<WritableClass> values, Context context) throws IOException, InterruptedException {

        long total = 0;
        double averageWS = 0.0;

        for(WritableClass v : values) {
            total += v.getTotalCount();
            averageWS += v.getAverageWindSpeed();
        }

        newTuple.setTotalCount(total);
        newTuple.setAverageWindSpeed(averageWS);

        context.write(key, newTuple);

    }
}