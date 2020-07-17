package com.hadoop.finalProject.Q6;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

public class ReducerClass extends Reducer<NullWritable, Text, NullWritable, Text> {

    static class DescOrder implements Comparator<Integer> {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o2.compareTo(o1);
        }
    }

    private TreeMap<Integer, Text> countMap = new TreeMap<>(new DescOrder());

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {

            String[] tokens = value.toString().split("\t");
            int countOfStates = Integer.parseInt(tokens[1]);

            countMap.put(countOfStates, new Text(value));


            if (countMap.size() > 10)
                countMap.remove(countMap.lastKey());
        }

        for (Text t : countMap.values())
            context.write(NullWritable.get(), t);
    }
}