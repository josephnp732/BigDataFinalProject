package com.hadoop.finalProject.Q10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "ReduceSideJoin");
        job.setJarByClass(DriverClass.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MainMapperClass.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StateMapperClass.class);

        job.getConfiguration().set("join.type", "inner");
        job.setReducerClass(ReducerClass.class);

        // Partitioner Class
        job.setPartitionerClass(PartitionerClass.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        Path outDir = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outDir);

        // US States
        job.setNumReduceTasks(49);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
