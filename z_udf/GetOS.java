package com.yahoo.tp.mi_udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class GetOS extends EvalFunc<String>
{
	public String exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null || input.size() == 0)
			return null;
		try{
			String str = input.get(0).toString();
			if ( str == null ){
				return null;
			}else{
				if (( str.indexOf("Windows NT 5.0") >= 0 ) && ( str.indexOf("Windows NT 5.1" ) < 0))
					return "Windows2000";
				else if (( str.indexOf("Windows NT 5.1") >= 0 ) && (str.indexOf("Windows NT 5.0")) < 0 && ( str.indexOf("Windows NT 6.0" ) < 0))
					return "Windows XP";
				else if ( str.indexOf("Windows NT 5.2") >= 0 )
					return "Windows 2003";
				else if (( str.indexOf("Windows NT 6.0") >= 0 ) && (str.indexOf("Windows NT 5.1") < 0))
					return "Windows Vista/Windows Server 2008";
				else if ( str.indexOf("Windows NT 6.1") >= 0 )
					return "Windows 7/Windows Server 2008 R2";			
				else if ((( str.indexOf("Win 9x") >= 0 ) && str.indexOf("Windows 98") < 0 && ( str.indexOf("Win98" ) < 0 )) || ( str.indexOf("Win95") >= 0 ) || ( str.indexOf("Windows 95") >= 0 ) || ( str.indexOf("Win98") >= 0 ) || ( str.indexOf("Windows 98") >= 0 ) || ( str.indexOf("Windows2000") >= 0 ))
					return "Rare OS";
				else if (( str.indexOf("Win3.11") >= 0 ) || ( str.indexOf("Windows 3.1") >= 0 ))
					return "Windows 3.1";
				else if ( str.indexOf("Win32") >= 0 )
					return "Windows 32";
				else if ( str.indexOf("Windows NT 4.0") >= 0 )
					return "Windows NT 4.0";
				else if ( str.indexOf("Windows CE") >= 0 )
					return "Windows CE";
				else if ( str.indexOf("Windows NT 5.01") >= 0 )
					return "Windows NT 5.01";
				else if ( str.indexOf("WinNT") >= 0 )
					return "WinNT";
				else if ( str.indexOf("Mac") >= 0 )
					return "Mac";
				else if ( str.indexOf("HP-UX") >= 0 )
					return "HP-UX";
				else if ( str.indexOf("SunOS") >= 0 )
					return "SunOS";
				else if ( str.indexOf("Linux") >= 0 )
					return "Linux";
				else
					return "Others";
			}
		}catch(Exception e){
			throw new IOException("Caught exception processing input row " + e);
		}
	}
}