package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import java.io.IOException;

// Input: (double)value series
// Output: mean, sd, cv
public class GetMeanSdCv extends EvalFunc<Tuple> {
	
	public Tuple exec(Tuple tuple) throws IOException {

		Tuple stat = TupleFactory.getInstance().newTuple(3);
		
		// return same field count
		if (tuple == null || tuple.size() < 1) {
    		stat.set(0, null);
    		stat.set(1, null);
    		stat.set(2, null);
    		return stat;
		}

		DataBag featureValueBag = (DataBag) tuple.get(0);

		long size = 0;
		double totalValue = 0;
		double squareValue = 0;
		double meanValue = 0;
		double sdValue = 0;
		double cvValue = 0;
		
		for (Tuple featureValue : featureValueBag) {
			if(featureValue.get(0) != null) {
				double value = ((Double) (featureValue.get(0))).doubleValue();
				totalValue += value;
				squareValue += value * value;
				size++;
			}
		}

        if (size > 1) {
        	meanValue = totalValue / size;	// return 1: mean
            sdValue = Math.abs((squareValue / size) - (meanValue * meanValue));
            sdValue = Math.sqrt(sdValue);	// return 2: sd
            if (meanValue > 0) {
                cvValue = sdValue / meanValue;
                cvValue /= Math.sqrt(size - 1);	// return 3: cv
            } else {
                cvValue = 1;
            }
        } else if (size == 1){
        	meanValue = totalValue;
        	sdValue = 0;
            cvValue = 1;
        } else {
    		stat.set(0, null);
    		stat.set(1, null);
    		stat.set(2, null);
    		return stat;
        }

		stat.set(0, meanValue);
		stat.set(1, sdValue);
		stat.set(2, cvValue);
		return stat;
	}
}
