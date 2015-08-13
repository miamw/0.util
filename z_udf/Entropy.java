package com.yahoo.tp.mi_udf;

import java.io.IOException;
import java.util.HashMap;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
/**
 * compute entropy<br>
 * the input databag should not be too large
 * */
public class Entropy extends EvalFunc<Double> {

	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		
		DataBag cnts = (DataBag) input.get(0);
		
		long num=0; 
		double total=0;
		
		// Get distribution of input keys
		HashMap<String,Integer> dist = new HashMap<String,Integer>();
		for(Tuple cnt :cnts){
			String curr=(String)cnt.get(0);
			if(curr!=null && curr!="0"){//skip null and zero value
					total++;
					dist.put(curr,dist.get(curr)+1);
			}
		}
		
		// get entropy of hash: dist
		if(total==0)return null;
		if(total==1)return (new Double(0)); //or 1?
		double entropy=0;
		for(String key : dist.keySet()) {
			num = dist.get(key);
			Double prob=num/total;
			entropy+=-prob*Math.log(prob);	
		}
		entropy/=Math.log(num);
		return entropy;
	}
}
