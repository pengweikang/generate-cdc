package com.pwk.generatesql.core;

import com.pwk.generatesql.db.DBUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
*@author		create by pengweikang
*@date		2018年3月29日--下午4:20:15
*@problem
*@answer
*@action
*/
public class TableFactory {
	
	static List<String>  list = new ArrayList<String>();
	
	
	
	
	public static List<String> getTableList() throws Exception{
		list.add("COMP");
		list.add("COMP_BUSINESS");
		list.add("EMP");
		list.add("EMP_BUSINESS");
		list.add("EMP_CERTIF");
		list.add("FM_SYS_USER");
		list.add("SYS_DISTRICT");
		list.add("VHCL");
		list.add("VHCL_BUSINESS");
		list.add("SYS_TYPE_CZ");
		
		List<String> tList = new ArrayList<String>();
		String SQL = "select * from tab where tabtype ='TABLE'";
		Connection conn = DBUtils.getConnection();
		
		PreparedStatement ps = conn.prepareStatement(SQL);
		
		ResultSet rs =ps.executeQuery();
		
		
		while(rs.next()) {
			String name = rs.getString("TNAME");
			if(!name.startsWith("BIN$")) {
				if(list.indexOf(name) > -1) {
					tList.add(name);
				}
				
			}
				
		}
		DBUtils.close(null, null, conn);
		return tList;
	}
	
	
	public static void main(String[] args)throws Exception {
		System.out.println(getTableList());
	}
	
	

}
