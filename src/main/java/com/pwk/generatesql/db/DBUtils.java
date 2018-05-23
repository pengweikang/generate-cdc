package com.pwk.generatesql.db;

import com.pwk.generatesql.utils.Params;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class DBUtils {
	
	
	private static String url;
	private static String user;
	private static String passwd;
	private static String driverClass;
	
	
	
	
	static {
		
		Properties properties = new Properties();
		
		InputStream inputStream = DBUtils.class.getResourceAsStream("/jdbc.properties");
	
		try {
			properties.load(inputStream);
			
			driverClass = properties.getProperty("jdbc.driverClassName");
			url = properties.getProperty("jdbc.url");
			user = properties.getProperty("jdbc.username");
			passwd = properties.getProperty("jdbc.password");
			inputStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void close(ResultSet rs,Statement st,Connection conn) {
		
		try {
			if(rs != null) {
				rs.close();
				rs = null;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			if(st != null) {
				st.close();
				st = null;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			if(conn != null) {
				conn.close();
				conn = null;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	
	
	
	public static Connection getConnection() throws SQLException {
		
		Connection connection = DriverManager.getConnection(url,user,passwd);
		return connection;
	}
	
	/**
	 * pengweikang 20180329 获取发布者连接
	 * @return
	 * @throws SQLException
	 */
	public static Connection getPublisherConnection () throws SQLException {
		Connection connection = DriverManager.getConnection(url, Params.publisherAccount,Params.publisherPasswd);
		return connection;
	}
	
	
	/**
	 * pengweikang 20180329 获取订阅者连接
	 * @return
	 * @throws SQLException
	 */
	public static Connection getSubscriberConnection () throws SQLException {
		Connection connection = DriverManager.getConnection(url,Params.subscriberAccount,Params.subscriberPasswd);
		return connection;
	}

}
