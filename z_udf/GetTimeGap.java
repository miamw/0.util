package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import java.io.IOException;

//Input: time series
//Output: cnt, mean, sd, cv
public class GetTimeGap extends EvalFunc<Tuple>  {
	private double MAX_SESSION_IDLE_TIME = 600; 
	
	public GetTimeGap(String idle_time) throws Exception {
		Double max_session_idle_time = Double.valueOf(idle_time);
		if(max_session_idle_time <= 0) {
			return;
		}
		MAX_SESSION_IDLE_TIME = max_session_idle_time;
	}
	
	public Tuple exec(Tuple tuple) throws IOException {

		Tuple stat = TupleFactory.getInstance().newTuple(4);
		
		// return same field count
		if (tuple == null || tuple.size() < 1) {
    		stat.set(0, 0);
    		stat.set(1, null);
    		stat.set(2, null);
    		stat.set(3, null);
    		return stat;
		}
		
		// check if sorted
		DataBag timestamps = (DataBag) tuple.get(0);
		
		long size = 0;
		double previousTimestamp = 0;
        double timeDiff = 0;
        double totalValue = 0;
        double squareValue = 0;
        double meanValue = 0;
		double sdValue = 0;
		double cvValue = 0;
        
        for (Tuple timestamp : timestamps) {
            double currentTimestamp = ((Double) (timestamp.get(0))).doubleValue();
            if (previousTimestamp == 0) {
            	timeDiff = -1;
            } else {
            	timeDiff = currentTimestamp - previousTimestamp;
            }

            if (timeDiff >= 0 && timeDiff <= MAX_SESSION_IDLE_TIME) {
                totalValue += timeDiff;
                squareValue += timeDiff * timeDiff;
                size++;	// return 1: cnt
            }
            previousTimestamp = currentTimestamp;
        }

        if (size > 1) {
        	meanValue = totalValue / size;	// return 2: mean
            sdValue = Math.abs((squareValue / size) - (meanValue * meanValue));
            sdValue = Math.sqrt(sdValue);	// return 3: sd
            if (meanValue > 0) {
                cvValue = sdValue / meanValue;
                cvValue /= Math.sqrt(size - 1);	// return 4: cv
            } else {
                cvValue = 1;
            }
        } else if (size == 1){
        	meanValue = totalValue;
        	sdValue = 0;
            cvValue = 1;
        } else {
    		stat.set(0, 0);
    		stat.set(1, null);
    		stat.set(2, null);
    		stat.set(3, null);
    		return stat;
        }
        
		stat.set(0, size);
		stat.set(1, meanValue);
		stat.set(2, sdValue);
		stat.set(3, cvValue);
		return stat;
	}
}
