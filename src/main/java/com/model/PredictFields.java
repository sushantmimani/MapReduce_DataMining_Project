package com.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PredictFields {
	
	public static class PredictFieldsMapper extends Mapper<Object,Text,NullWritable,Text> {

        private MultipleOutputs mos;


        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }
        
        public static ArrayList<String> createDatasetLabelled(ArrayList<String> values){
        	ArrayList<String> al = new ArrayList<String>();
            al.addAll(values.subList(2,8));
            al.addAll(values.subList(19, 26));
            al.addAll(values.subList(27,953));
            al.addAll(values.subList(955, 1016));
            al.addAll(values.subList(1018,1656));
            al.add(values.get(values.size()-1));
            al.addAll(values.subList(26,27));
            return al;
        }
        
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
        	ArrayList<String> values = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
        		ArrayList<String> al = createDatasetLabelled(values);
	            if(!values.get(0).trim().equals("SAMPLING_EVENT_ID")) {
	                if(!StringUtils.isNumeric(al.get(31).trim())){
	                    al.set(31, "0");
	                }
	                    if(StringUtils.isNumeric(al.get(31).trim()) && (Integer.parseInt(al.get(31).trim())>0)){
	                    al.set(31, "1");
	                }
	                String val = al.toString();
	                val = val.substring(1,val.length()-1);
	                val = val.replaceAll("\\?", "0");
	                context.write(NullWritable.get(), new Text(val));
	            }
	            
	            else {
	            	ArrayList<String> values1 = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
	            	ArrayList<String> al1 = createDatasetLabelled(values1);
	            	System.out.println(al1);
	            	return;
	            }
	            
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException, Exception {
		Configuration conf = new Configuration();
		Job job4 = Job.getInstance(conf, "Predict fields");
		job4.setJarByClass(PredictFields.class);
	    job4.setMapperClass(PredictFieldsMapper.class);
	    job4.setNumReduceTasks(0);
	    job4.setOutputKeyClass(NullWritable.class);
	    job4.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job4, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job4,new Path(args[1]+"/allFields"));
	    if (!job4.waitForCompletion(true)) {
	        throw new Exception("Job failed");
	      }
	}

}
