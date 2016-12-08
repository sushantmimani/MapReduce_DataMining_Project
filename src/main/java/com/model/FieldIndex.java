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
	        for(int j=0; j<values.size();j++)
	        	System.out.println(values.get(j)+":"+j);
	        al.addAll(values.subList(2,3));
            al.addAll(values.subList(5, 6));
            al.addAll(values.subList(7,8));
            al.addAll(values.subList(957, 958));
            al.addAll(values.subList(963,968));
            al.addAll(values.subList(1015,1016));
            al.addAll(values.subList(1024,1026));
            al.addAll(values.subList(1036,1038));
            al.addAll(values.subList(1050,1053));
            al.addAll(values.subList(1058,1059));
            al.addAll(values.subList(1061,1063));
            al.addAll(values.subList(1073,1075));
            al.addAll(values.subList(1076,1077));
            al.addAll(values.subList(1082,1083));
            al.addAll(values.subList(1092,1098));
            al.addAll(values.subList(1101,1102));
            al.addAll(values.subList(26,27));
	        i++;
		    }
	        PrintWriter writer = new PrintWriter("/Users/sushantmimani/Documents/NEU/MR/Project/header.txt", "UTF-8");
	        writer.println(al);
	        writer.close();
		}
}
	

