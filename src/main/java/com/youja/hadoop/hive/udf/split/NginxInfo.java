package com.youja.hadoop.hive.udf.split;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NginxInfo {
	public static String getInfo( String str ) throws ParseException{
		
		String strs[] = str.split(" ");
		if (strs.length < 10){
			return null;
		}

		String outStr = null;	
		String uInfos[] = strs[6].split("\\?");
		String  api = null ;
		String info = "\001";
		if (uInfos.length >=2 )
		{
			api = uInfos[0];
			String infoItems[] = uInfos[1].split("&");
			
			for (int i=0;i<infoItems.length;i++)
			{
				String items[] = infoItems[i].split("=");
				if (items.length >=2)
				{ 
					if (i == 0 )
					{
						info = items[0] + '\003' + items[1] + info;
						if (items[0].equals("token")){
							info = "TokenUserId" + '\003' + TokenToUserId(items[1]) + '\002' + info;
						}
					}else
					{
						info = items[0] + '\003' + items[1] + '\002' + info;
						if (items[0].equals("token")){
							info = "TokenUserId" + '\003' + TokenToUserId(items[1])  + '\002' + info;
						}
					}
				}
			}
		}else
		{
			api=strs[6];
		}
		outStr = strs[0] + '\001' + getTimeStamp(str)  + '\001' + strs[5].substring(1) + '\001' + info + strs[8] + '\001' +strs[9] + '\001' +  api ;

		return outStr;
	}
	
	public static String getTimeStamp( String lineTxt ) throws ParseException{
        String regexTime = "\\[(.*?)\\]";
        Pattern patternTime = Pattern.compile(regexTime);
        Matcher matcherTime = patternTime.matcher(lineTxt);
        if(matcherTime.find()){
        	String time = matcherTime.group(1);
        	time = time.replaceAll("Jan", "01");
        	time = time.replaceAll("Feb", "02");
        	time = time.replaceAll("Mar", "03");
        	time = time.replaceAll("Apr", "04");
        	time = time.replaceAll("May", "05");
        	time = time.replaceAll("June", "06");
        	time = time.replaceAll("July", "07");
        	time = time.replaceAll("Aug", "08");
        	time = time.replaceAll("Sep", "09");
        	time = time.replaceAll("Oct", "10");
        	time = time.replaceAll("Nov", "11");
        	time = time.replaceAll("Dec", "12");
        	
        	String[] split = time.split("\\s");
        	String GMT = "GMT+8";
        	if(split[1].equals("+0100")){
        		GMT = "GMT+1";
        	}
        	else if(split[1].equals("+0200")){
        		GMT = "GMT+2";
        	}
        	else if(split[1].equals("+0300")){
        		GMT = "GMT+3";
        	}                    	
        	else if(split[1].equals("+0400")){
        		GMT = "GMT+4";
        	}
        	else if(split[1].equals("+0500")){
        		GMT = "GMT+5";
        	} 
        	else if(split[1].equals("+0600")){
        		GMT = "GMT+6";
        	}
        	else if(split[1].equals("+0700")){
        		GMT = "GMT+7";
        	}
        	else if(split[1].equals("+0900")){
        		GMT = "GMT+9";
        	}
        	else if(split[1].equals("+1000")){
        		GMT = "GMT+10";
        	}
        	else if(split[1].equals("+1100")){
        		GMT = "GMT+11";
        	}
        	else if(split[1].equals("+1200")){
        		GMT = "GMT+12";
        	}
        	SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        	sdf.setTimeZone(TimeZone.getTimeZone(GMT));
        	Date date = sdf.parse(split[0]);
        	Timestamp ts = new Timestamp(date.getTime());
        	String timestamp = Long.toString(ts.getTime());
            return timestamp.substring(0, timestamp.length()-3);
        }
		return null;
	}
	
	private static String TokenToUserId(String token){
		String items[]= token.split("-");
		String userid = null ;
		if(items.length >=2)
		{
			userid = items[1];
		}
		return userid;
	}
}
