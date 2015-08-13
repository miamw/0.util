package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class GetDomain extends EvalFunc<String> {
	private String url;
	private String domain;
	
	public String exec(Tuple input) throws IOException {		
		if (input == null || input.get(0) == null || input.size() == 0)
			return null;
		try{
				url = input.get(0).toString();
				if(url == null) {
					return null;
				}else{
					domain = url.replaceAll("^https?://(www.)?", "").replaceAll("/.*", "").replaceAll("[?#:].*", "");
					return domain;
				}
		}catch(Exception e){
			throw new IOException("Exception: " + e + " Url: " + url + " Domain: " + domain);
		}
	}
}