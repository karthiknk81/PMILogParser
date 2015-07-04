package com.parser.log.pmi.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
public class ToolSet {
	

	public static String convertUnixToCST_V1(long argUnixTime){
		
		Date date = new Date(argUnixTime*1000L); // *1000 is to convert seconds to milliseconds
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
		sdf.setTimeZone(TimeZone.getTimeZone("GMT-5")); // give a timezone reference for formating (see comment at the bottom
		return sdf.format(date);
		
	}
	
	public static String convertUnixToCST_V2(long argUnixTime){
		System.out.println(argUnixTime);
		return Long.toString((argUnixTime/86400)+25569+((-5)/24));
	}
	
}
