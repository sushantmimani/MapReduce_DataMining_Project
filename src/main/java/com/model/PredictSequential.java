package com.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;

import weka.classifiers.Classifier;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;
import weka.filters.unsupervised.attribute.StringToNominal;

public class PredictSequential {
public static void main(String[] args) throws Exception {
	
	
	Classifier cls =(Classifier) SerializationHelper.read(args[0]);

	// load unlabeled data
	
	File f = new File("/Users/sushantmimani/Documents/NEU/MR/Project/output/unlabeledData/test.csv");
	CSVLoader cnv = new CSVLoader();
	cnv.setSource(f);
	Instances unlabeled = cnv.getDataSet();
	
//	 StringToNominal convert= new StringToNominal();
//     String[] options= new String[2];
//     options[0]="-R";
//     options[1]="32";  //range of variables to make numeric
//     convert.setOptions(options);
//     convert.setInputFormat(unlabeled);
//     Instances labeled=Filter.useFilter(unlabeled, convert);
	 
	 
	 // create copy
	 Instances labeled = new Instances(unlabeled);
	 
	 
	// set class attribute
	labeled.setClassIndex(labeled.numAttributes() - 1);
	System.out.println(labeled.checkForStringAttributes());
	System.out.println(labeled.classAttribute());

	// label instances
	 for (int i = 0; i < labeled.numInstances(); i++) {
		 
	   double clsLabel =  cls.classifyInstance(labeled.instance(i));
	   labeled.instance(i).setClassValue(clsLabel);
	   System.out.println(labeled.instance(i).classValue());
	 }
	 
	 // save labeled data
//	 BufferedWriter writer = new BufferedWriter(
//	                           new FileWriter(args[1]));
//	 writer.write(labeled.toString());
//	 writer.newLine();
//	 writer.flush();
//	 writer.close();

}
	

}
