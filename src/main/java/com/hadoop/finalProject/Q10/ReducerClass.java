package com.hadoop.finalProject.Q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, WritableClass, Text, WritableClass> {

    WritableClass newTuple = new WritableClass();

    @Override
    protected void reduce(Text key, Iterable<WritableClass> values, Context context) throws IOException, InterruptedException {

        long total = 0;
        double sum = 0.0;

        for(WritableClass v : values) {
            total += v.getTotalCount();
            sum += v.getAverageWindSpeed();
        }

        double averageWS = sum / total;

        double rms = Math.sqrt(Math.pow(sum, 2) / total);

        newTuple.setTotalCount(total);
        newTuple.setAverageWindSpeed(averageWS);
        newTuple.setRms(rms);

        context.write(key, newTuple);

    }
}
