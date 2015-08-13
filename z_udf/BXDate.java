
package extractcookietime;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import org.apache.pig.impl.util.WrappedIOException;

public class BXDate extends EvalFunc <Long> {
    private int yax_decode32_char(char encodeChar) {
        int retval;
        encodeChar = Character.toLowerCase(encodeChar);

        if(Character.isLetter(encodeChar) && encodeChar <= 'v') {
            retval = encodeChar - ('a' - 10);
        } else if(Character.isDigit(encodeChar)) {
            retval = encodeChar - '0';
        } else {
           retval = 0;
        }
        return retval;
    }

    private long yax_decode32(String encoded_num, int len) {
        long retVal = 0;
        int i;

        if(encoded_num.length() > 0) {
            retVal += yax_decode32_char(encoded_num.charAt(0));
            for(i = 1; i < len; i++) {
                retVal <<= 5;
                retVal |= yax_decode32_char(encoded_num.charAt(i));
            }
        }
        return retVal;
    }

    private int extract_timestamp_from_browserID(String browserID, int browserID_len) {
        if (browserID_len > 13) {
            browserID_len = 13;
        }
        long browserID_decode32_val = yax_decode32(browserID, browserID_len);
        return (int)(browserID_decode32_val & 0xFFFFFFFF);
    }

    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            long time;
            String str = (String)input.get(0);
	    if(str == null)
		return null;
            time = extract_timestamp_from_browserID(str, str.length());
            return time;
        }catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}

