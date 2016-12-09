package com.model;

import java.io.File;

import weka.attributeSelection.CfsSubsetEval;
import weka.attributeSelection.GreedyStepwise;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;
import weka.filters.unsupervised.attribute.StringToNominal;

public class GetFields {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		weka.filters.supervised.attribute.AttributeSelection filter = new weka.filters.supervised.attribute.AttributeSelection();
		File f = new File("/Users/sushantmimani/Documents/NEU/MR/Project/output/allFields/all.csv");
		CSVLoader cnv = new CSVLoader();
		cnv.setSource(f);
		Instances data = cnv.getDataSet();
		System.out.println("Dataset loaded");
		StringToNominal convert= new StringToNominal();
        Instances testInstances1 =null;
        try {
 
            convert.setInputFormat(data);
            testInstances1 = Filter.useFilter(data, convert);
        } catch (Exception e) {
			// TODO: handle exception
		}
    	System.out.println("Nominal done");
        testInstances1.setClassIndex(testInstances1.numAttributes()-1);
		CfsSubsetEval eval = new CfsSubsetEval();
	    GreedyStepwise search = new GreedyStepwise();
	    search.setSearchBackwards(true);
	    filter.setEvaluator(eval);
	    filter.setSearch(search);
	    filter.setInputFormat(testInstances1);
	    Instances newData = Filter.useFilter(testInstances1, filter);
	    System.out.println(newData.enumerateAttributes());

	}

}
