package com.yahoo.tp.mi_udf;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FilterRecord extends FilterFunc {
	private List<String> idList;
	private static final String DELIMITER = ",";
	
	public FilterRecord(String ids) throws Exception {
		if(ids == null)
			return;
		String[] tmp = ids.split(DELIMITER);
		idList = new ArrayList<String>(Arrays.asList(tmp));
	}
	
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null || input.size() == 0)
			return false;
		try{
			String id = input.get(0).toString();
			if(idList.contains(id))
				return true;
			else			
				return false;
		}catch(Exception e){
			throw new IOException("Caught exception processing input row " + e);
		}
	}
}