package com.yahoo.tp.mi_udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
/**
 * compute entropy<br>
 * the input databag should not be too large
 * */
public class Cv extends EvalFunc<Double> {

	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		
		DataBag cnts = (DataBag) input.get(0);

		// get overall numbers
		double num=0; 
		double total=0;
		for(Tuple cnt :cnts){
			Double curr=(Double)cnt.get(0);
			if(curr!=null && curr!=0){//skip null and zero value
				num++;
				total+=curr;
			}
		}
		double mean=total/num;

		// get cv
		if(total==0 || num==0)return null;
		if(num==1)return (new Double(-1)); //or 1?
		double std=0, cv=0;
		for(Tuple cnt :cnts){
			Double curr=(Double)cnt.get(0);
			if(curr!=null && curr!=0){
				Double a=(curr-mean)*(curr-mean);
				std+=a;	
			}
		}
		std = Math.sqrt(std/num);
		cv = std/mean;
		return cv;
	}
}
