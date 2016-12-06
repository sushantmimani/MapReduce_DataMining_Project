package com.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Naomi on 12/3/16.
 */
public class Driver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        conf.set("mapred.textoutputformat.separator", ":");
        if (otherArgs.length < 2) {
            System.err.println("Usage: PageRank <in> [<in>...] <out>");
            System.exit(2);
        }
////      Job to call the parser
//        Job job = Job.getInstance(conf, "Extract header");
//        job.setJarByClass(Driver.class);
//        job.setMapperClass(ViewData.DataMapper.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(0);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job,
//                new Path(otherArgs[1]+"/processedData"));
//        MultipleOutputs.addNamedOutput(job, "header", TextOutputFormat.class, NullWritable.class, Text.class);
//        MultipleOutputs.addNamedOutput(job, "data", TextOutputFormat.class, NullWritable.class, Text.class);
//        boolean ok = job.waitForCompletion(true);
//        if (!ok) {
//            throw new Exception("Job failed");
//        }

//      Job to call the parser
        Job job1 = Job.getInstance(conf, "Train model");
        job1.setJarByClass(WekaModel.class);
        job1.setMapperClass(WekaModel.WekaMapper.class);
        job1.setReducerClass(WekaModel.WekaReducer.class);
        job1.addCacheFile(new Path(otherArgs[1]+"/processedData/header-m-00000").toUri());
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]+"/processedData"));
        FileOutputFormat.setOutputPath(job1,
                new Path(otherArgs[1]+"/arff"));
        if (!job1.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }
    }
}