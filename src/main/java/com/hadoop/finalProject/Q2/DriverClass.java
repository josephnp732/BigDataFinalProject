package com.hadoop.finalProject.Q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "NumberOfAccidentsPerHourOfDay");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator","\t");

        // Driver Class
        job.setJarByClass(DriverClass.class);

        job.setInputFormatClass(TextInputFormat.class);

        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);

        // Comparator
        job.setSortComparatorClass(ComparatorClass.class);

        // Mapper
        job.setMapperClass(MapperClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer
        job.setReducerClass(ReducerClass.class);

        // Create only 1 reducer
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }


        // Submit the job, then poll for progress until the job is complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

