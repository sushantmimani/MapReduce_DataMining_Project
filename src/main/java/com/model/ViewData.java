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


        public void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<String> values = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
            if(!values.get(0).trim().equals("LATITUDE")) {
                ArrayList<String> al = new ArrayList<String>();
                al.addAll(values.subList(2,8));
                al.addAll(values.subList(955, 961));
                al.addAll(values.subList(962,1016));
                al.addAll(values.subList(1019, 1102));
                al.addAll(values.subList(26,27));
                if(!StringUtils.isNumeric(al.get(149).trim())){
                    al.set(149, "0");
                }if(StringUtils.isNumeric(al.get(149).trim()) && (Integer.parseInt(al.get(149).trim())>0)){
                    al.set(149, "1");
                }
                String val = al.toString();
                val = val.substring(1,val.length()-1);
                val = val.replaceAll("\\?", "0");
                mos.write("data",NullWritable.get(), new Text(val));
            }
            else {
                ArrayList<String> al1 = new ArrayList<String>();
                al1.addAll(values.subList(2,8));
                al1.addAll(values.subList(955, 961));
                al1.addAll(values.subList(962,1016));
                al1.addAll(values.subList(1019, 1102));
                al1.addAll(values.subList(26,27));
                String val1 = al1.toString();
                val1 = val1.substring(1,val1.length()-1);
                mos.write("header",NullWritable.get(), new Text(val1));
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
        
}