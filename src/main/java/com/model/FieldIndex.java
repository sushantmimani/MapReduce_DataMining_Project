package com.model;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FieldIndex {

	public static void main(String[] args) throws CompressorException, IOException {
		// TODO Auto-generated method stub

		FileInputStream fin = new FileInputStream(args[0]);
	    BufferedInputStream bis = new BufferedInputStream(fin);
	    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
	    BufferedReader br = new BufferedReader(new InputStreamReader(input));
	    int i=0;
	    String line;
	    ArrayList<String> al = new ArrayList<String>();
	    while (i<1 && (line = br.readLine()) != null) {
	        String[] fields = line.split(",");
	        ArrayList<String> values = new ArrayList<String>(Arrays.asList(fields));
            al.addAll(values.subList(0, 19));
            al.addAll(values.subList(26,27));
            al.addAll(values.subList(955, 1016));
            al.addAll(values.subList(1018, values.size()));
	        i++;
		    }
	        PrintWriter writer = new PrintWriter("/Users/sushantmimani/Documents/NEU/MR/MapReduce_DataMining_Project/header.txt", "UTF-8");
	        writer.println(al);
	        writer.close();
		}

//	public static class HeaderMapper extends Mapper<Object,Text,NullWritable,Text> {
//		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			ArrayList<String> values = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
//			if(values.get(0).equals("SAMPLING_EVENT_ID")) {
//				ArrayList<String> al = new ArrayList<String>();
//				al.addAll(values.subList(0, 19));
//				al.addAll(values.subList(26,27));
//				al.addAll(values.subList(955, 1016));
//				al.addAll(values.subList(1018, values.size()));
//				String val = al.toString();
//				val = val.substring(1,val.length()-1);
//				context.write(NullWritable.get(), new Text(val));
//			}
//		}

		}
	

