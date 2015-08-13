package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.Arrays;

public class GetTopTraffic extends EvalFunc<String>
{	
	private static final String DELIMITER = ",";
	private float topN;

	public GetTopTraffic(String topNTmp) throws Exception {
		if( topNTmp == null || topNTmp.equals(null) || Float.valueOf(topNTmp) > 1 || Float.valueOf(topNTmp) <0 )
			return;
		topN = Float.valueOf(topNTmp);
	}	
	
	public String exec(Tuple input) throws IOException {
		int sumTop=0;
		int sumAll=0;
		int top=0;
		String topPerc=null;
		String allNum=null;
		String allNumNml=null;
//		String tmp=null;
				
		if (input == null || input.get(0) == null || input.size() == 0)
			return null;
		try{
			allNum=input.get(0).toString();
			
			// init number array
			allNumNml = allNum.replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\\{", "").replaceAll("\\}", "");
			if (allNumNml.equals("") || allNumNml.equals(null)) {
				return null;
			}
	
			String[] allNumArr1 = allNumNml.split(DELIMITER);
			int allNumArr[]=new int[allNumArr1.length];
			for(int i=0;i<allNumArr1.length;i++) {
				if(allNumArr1[i].equals(null)) {
					allNumArr1[i]="0";
				}
				allNumArr[i]=Integer.parseInt(allNumArr1[i]);
			}
			
			// sort and get ttl_cnt
			Arrays.sort(allNumArr);
			for(int i=0;i<allNumArr.length;i++) {
				sumAll+=allNumArr[i];
			}
			
			if(sumAll==0) {
				return null;
			}
			
			// get topN
			top=(int)(((float)allNumArr.length)*topN);
			if(top==0) {
				top=1;
			}
			
			for(int j=allNumArr.length-1;j>(allNumArr.length-1-top);j--) {
				sumTop+=allNumArr[j];
//				tmp=tmp+","+Integer.toString(allNumArr[j]);
			}
			
			topPerc=Float.toString((float)sumTop/(float)sumAll);
//			tmp=tmp+","+Integer.toString(sumAll)+","+topPerc;
//			return tmp;
			return topPerc;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row :" + input.get(0) + "," + allNumNml + "," +  + sumTop + "," + sumAll + "," + e);
		}
	}
}