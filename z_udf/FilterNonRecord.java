package com.yahoo.tp.mi_udf;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FilterNonRecord extends FilterFunc {
	private List<String> idList;
	private static final String DELIMITER = ",";
	
	public FilterNonRecord(String ids) throws Exception {
		if(ids == null)
			return;
		String[] tmp = ids.split(DELIMITER);
		idList = new ArrayList<String>(Arrays.asList(tmp));
	}
	
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null || input.size() == 0)
			return true;
		try{
			String id = input.get(0).toString();
			if(idList.contains(id))
				return false;
			else			
				return true;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row " + e);
		}
	}
}