package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class GetBrower extends EvalFunc<String>
{
	public String exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null || input.size() == 0)
			return null;
		try{
			String str = input.get(0).toString();
			if ( str == null ){
				return null;
			}else{
				if ( str.indexOf("Firefox") >= 0 )
					return "Firefox";
				else if (( str.indexOf("MSIE") >= 0 ) && ( str.indexOf("Win") >= 0 ))
					return "Win + MSIE UAs";
				else if ((( str.indexOf("chrome") >= 0 ) || ( str.indexOf("Chrome") >= 0 ) || ( str.indexOf("CHROME") >= 0 )) && (( str.indexOf("safari") >= 0 ) || ( str.indexOf("Safari") >= 0 ) || ( str.indexOf("SAFARI") >= 0 )))
					return "Chrome";
				else if ((( str.indexOf("version") >= 0 ) || ( str.indexOf("Version") >= 0 ) || ( str.indexOf("VERSION") >= 0 )) && (( str.indexOf("safari") >= 0 ) || ( str.indexOf("Safari") >= 0 ) || ( str.indexOf("SAFARI") >= 0 )))
					return "Safari";
				else if ( str.indexOf("Opera") >= 0 )
					return "Opera";
				else
					return "Others";
			}
		}catch(Exception e){
			throw new IOException("Caught exception processing input row " + e);
		}
	}
}