package com.hadoop.finalProject.Q8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<IntWritable, TempWritable, IntWritable, TempWritable> {

    TempWritable newTuple = new TempWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<TempWritable> values, Context context) throws IOException, InterruptedException {

        double maxTemp = Double.MIN_VALUE;
        double minTemp = Double.MAX_VALUE;
        long count = 0;
        double sumOfTemps = 0.0;

        for (TempWritable tuple : values) {

            sumOfTemps += tuple.getAverageTemp() * tuple.getCount();
            count += tuple.getCount();

            if(tuple.getMinTemp() < minTemp) {
                minTemp = tuple.getMinTemp();
            }

            if(tuple.getMaxTemp() > maxTemp) {
                maxTemp = tuple.getMaxTemp();
            }
        }

        newTuple.setAverageTemp(sumOfTemps / count);
        newTuple.setMaxTemp(maxTemp);
        newTuple.setMinTemp(minTemp);
        newTuple.setCount(count);

        context.write(key, newTuple);
    }
}
