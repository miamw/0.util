package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DiffTime extends EvalFunc<String>
{
	public String exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null || input.get(1) == null || input.size() == 0)
			return null;
		try{
			String time1 = input.get(0).toString();
			String time2 = input.get(1).toString();
			if(( time1 == null ) || ( time2 == null )) {
				return null;
			}else{
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
				Date date1 = sdf.parse(time1);
				Date date2 = sdf.parse(time2);
				long ms1 = date1.getTime() / 1000;
				long ms2 = date2.getTime() / 1000;
				long timeDiff = ms2-ms1;
				return Long.toString(timeDiff);
			}
		}catch(Exception e){
			throw new IOException("Caught exception processing input row " + e);
		}
	}
}