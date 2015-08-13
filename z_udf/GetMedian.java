package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.Arrays;

public class GetMedian extends EvalFunc<String> {
	private static final String DELIMITER = ",";
	private String nos;
	private Integer[] impcnt; 
	private String median;
	
	public String exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null || input.size() == 0)
			return null;
		try{
			nos = input.get(0).toString();
			if(nos == null) {
				return "0";
			}else{
				impcnt = new Integer[7];
				String nosNml = nos.replaceAll("\\(", "").replaceAll("\\)", "");
				String[] nosArr = nosNml.split(DELIMITER);
				for(int i=0;i<7;i++) {
					if(nosArr[i] == null)
						impcnt[i] = 0;
					else {
						impcnt[i] = Integer.valueOf(nosArr[i]);
					}
				}
			}
			
			Arrays.sort(impcnt);
			median=impcnt[3].toString();
			
			return median;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row: " + e + "all: " + input.get(0) + "1: " + impcnt[0] + "2: " + impcnt[1] + "7: " + impcnt[6]);
		}
	}
}