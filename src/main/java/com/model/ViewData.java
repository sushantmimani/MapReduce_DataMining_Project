package com.model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ViewData {

    public static class DataMapper extends Mapper<Object,Text,NullWritable,Text> {

        private MultipleOutputs mos;
        ArrayList<String> val;
        String header;

        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
            URI[] files = context.getCacheFiles();
			BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
			String strLineRead = "";
			while ((strLineRead = br.readLine()) != null) {
				val = new ArrayList<String>(Arrays.asList(strLineRead.split(",")));
			}
			header = val.toString().substring(2,val.size()-1);
			context.write(NullWritable.get(), new Text(header));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<String> values = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
            if(!values.get(0).trim().equals("SAMPLING_EVENT_ID")) {
                ArrayList<String> al = new ArrayList<String>();
                al.addAll(values.subList(0, 19));
                al.addAll(values.subList(26,27));
                al.addAll(values.subList(955, 1016));
                al.addAll(values.subList(1018, values.size()));
                if(!StringUtils.isNumeric(al.get(19).trim()))
                	al.set(19, "0");
                String val = al.toString();
                val = val.substring(1,val.length()-1);
                val = val.replaceAll("\\?", "0");
                mos.write("data",NullWritable.get(), new Text(val));
            }
            else {
                ArrayList<String> al1 = new ArrayList<String>();
                al1.addAll(values.subList(0, 19));
                al1.addAll(values.subList(26,27));
                al1.addAll(values.subList(955, 1016));
                al1.addAll(values.subList(1018, values.size()));
                String val1 = al1.toString();
                val1 = val1.substring(1,val1.length()-1);
                mos.write("header",NullWritable.get(), new Text(val1));
            }
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
        	mos.close();
        }
    }

    public static class DataReducer extends Reducer<NullWritable,Text,NullWritable,Text> {

        ArrayList<String> val;

        public void setup(Context context) throws IOException, InterruptedException {
            URI[] files = context.getCacheFiles();
            BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
            String strLineRead = "";
            while ((strLineRead = br.readLine()) != null) {
                val = new ArrayList<String>(Arrays.asList(strLineRead.split(",")));
            }
            String header = val.toString();
            header = header.substring(2,header.length()-1);
            System.out.println(header);
            context.write(NullWritable.get(), new Text(header));
        }
        public  void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values) {
                context.write(NullWritable.get(), value);
            }

        }
    }
}