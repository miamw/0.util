package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import java.io.IOException;

// Input: double series
// Output: mean, sd, cv
public class GetMean extends EvalFunc<Tuple> {
	
	public Tuple exec(Tuple tuple) throws IOException {

			if (null == tuple || tuple.size() < 1) {
				return null;
			}

			DataBag featureValueBag = (DataBag) tuple.get(0);

			double totalValue = 0.0;
			long size = featureValueBag.size();

			for (Tuple featureValue : featureValueBag) {
				double value = ((Double) (featureValue.get(0))).doubleValue();
				totalValue += value;
			}

			double meanValue = totalValue / size;

			Tuple features = TupleFactory.getInstance().newTuple(1);
			features.set(0, meanValue);
			return features;
		}
	}
