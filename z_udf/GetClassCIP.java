package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class GetClassCIP extends EvalFunc<String> {
	private static final String DELIMITER = "[.]";
	private String ip;
	private String ipClassC;
	
	public String exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null || input.size() == 0)
			return null;
		try{
			ip = input.get(0).toString();
			
			if (ip == null) {
				return null;
			}else if(ip.matches("(([0-9]){1,3}[.]){3}([0-9]){1,3}")) {
				String[] ipClasses = ip.split(DELIMITER);
				ipClassC = ipClasses[0] + "." + ipClasses[1] + "." + ipClasses[2];			
				return ipClassC;				
			}else{
				return ip;
			}
		}catch(Exception e){
			throw new IOException("Caught exception processing input row " + e + " IP: " + ip + " ClassC: " + ipClassC);
		}
	}
}