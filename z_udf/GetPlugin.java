package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetPlugin extends EvalFunc<String> {
	private List<String> pluginList;
	private static final String DELIMITER = ",";
	
	public GetPlugin(String plugins) throws Exception {
		if(plugins == null)
			return;
		String[] tmp = plugins.split(DELIMITER);
		pluginList = new ArrayList<String>(Arrays.asList(tmp));
	}
	
	public String exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null || input.size() == 0)
			return null;
		try{
			String str = input.get(0).toString();
			String flag = null;
			if( str == null ){
				return null;
			}else{
				for(int i=0;i<pluginList.size();i++) {
					if ( str.indexOf(pluginList.get(i)) >= 0 ) {
						flag = pluginList.get(i); 
						break;
						/* Return a list
						if ( flag == null ) {
							flag = pluginList.get(i);
						}else{
							flag = flag + "," + pluginList.get(i);
						}
						 */
					}
				}
				if (flag == null) {
					flag = "Others";
				}
			return flag;
			}
		}catch(Exception e){
			throw new IOException("Caught exception processing input row " + e);
		}
	}
}