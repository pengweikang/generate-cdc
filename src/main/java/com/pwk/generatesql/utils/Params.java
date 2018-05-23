package com.pwk.generatesql.utils;

import com.pwk.generatesql.db.DBUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
*@author		create by pengweikang
*@date		2018年3月29日--下午4:20:15
*@problem
*@answer
*@action
*/
public class Params {
	
	private static Map<String,String> keyMap = new HashMap<String,String>();
	
	
	public  static String publisherAccount = "cdc_publisher";
	public static String publisherPasswd = "cdc_publisher";
	
	
	public static String subscriberAccount = "cdc_subscriber";
	public static String subscriberPasswd = "cdc_subscriber";
	
	
	public static  String  getValue(String key) {
		return keyMap.get(key);
	}	
	
	static {
		Properties properties = new Properties();
		InputStream inputStream = DBUtils.class.getResourceAsStream("/jdbc.properties");

		try {
			properties.load(inputStream);
			Set<Object> keySet = properties.keySet();
			for(Object key:keySet) {
				keyMap.put(key.toString(), properties.getProperty(key.toString()));
			}
			inputStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
