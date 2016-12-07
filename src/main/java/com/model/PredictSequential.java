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
import weka.core.converters.CSVLoader;

public class PredictSequential {
public static void main(String[] args) throws Exception {
	ObjectInputStream ois = new ObjectInputStream(new FileInputStream(args[0]));
	Classifier cls = (Classifier) ois.readObject();
	ois.close();
	// load unlabeled data
	
	File f = new File("/Users/sushantmimani/Documents/NEU/MR/Project/output/labeledData/data-m-00000");
	CSVLoader cnv = new CSVLoader();
	cnv.setSource(f);
	Instances unlabeled = cnv.getDataSet();
//	BufferedReader br = new BufferedReader(new FileReader("/some/where/unlabeled.arff"));
//	 Instances unlabeled = new Instances(br);
	 
	 // set class attribute
	 unlabeled.setClassIndex(unlabeled.numAttributes() - 1);
	 
	 // create copy
	 Instances labeled = new Instances(unlabeled);
	 
	 // label instances
	 for (int i = 0; i < unlabeled.numInstances(); i++) {
	   int clsLabel = (int) cls.classifyInstance(unlabeled.instance(i));
	   System.out.println(clsLabel);
	   labeled.instance(i).setClassValue(clsLabel);
	 }
	 // save labeled data
	 BufferedWriter writer = new BufferedWriter(
	                           new FileWriter(args[1]));
	 writer.write(labeled.toString());
	 writer.newLine();
	 writer.flush();
	 writer.close();

}
	

}
