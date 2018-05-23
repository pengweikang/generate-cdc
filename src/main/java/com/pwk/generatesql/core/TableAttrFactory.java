package com.pwk.generatesql.core;

import com.pwk.generatesql.bean.TableAttr;
import com.pwk.generatesql.db.DBUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
*@author		create by pengweikang
*@date		2018年3月29日--下午4:20:15
*@problem
*@answer
*@action
*/
public class TableAttrFactory {
	
	
	public static Map<String,List<TableAttr>> getTableAttr(List<String> tableList) throws Exception{
		
		Connection conn = DBUtils.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String,List<TableAttr>> map = new HashMap<String,List<TableAttr>>();
		for(String tableName : tableList) {
			
			String tempSql = "select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE,COLUMN_ID from user_tab_columns where table_name =UPPER(?)";
			ps = conn.prepareStatement(tempSql);
			ps.setString(1, tableName);
			System.out.println(tableName);
			rs = ps.executeQuery();
			List<TableAttr> list = new ArrayList<TableAttr>();
			while(rs.next()) {
				TableAttr attr = new TableAttr();
				attr.setColumnName(rs.getString("COLUMN_NAME"));
				attr.setColumnId(rs.getString("COLUMN_ID"));
				attr.setDataType(rs.getString("DATA_TYPE"));
				attr.setData_length(rs.getInt("DATA_LENGTH"));
				attr.setNullable(rs.getString("NULLABLE"));
				list.add(attr);
			}
			map.put(tableName, list);
		}
		DBUtils.close(rs, ps, conn);
		return map;
	}
	
	
public static Map<String,String> getTablePrimaryKey(List<String> tableList) throws Exception{
	
		Connection conn = DBUtils.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String,String> map = new HashMap<String,String>();
		for(String tableName : tableList) {
			
			String tempSql = "select COLUMN_NAME from user_cons_columns " + 
					"where constraint_name = (select constraint_name from user_constraints " + 
					" where table_name = ? and constraint_type='P')";
			ps = conn.prepareStatement(tempSql);
			ps.setString(1, tableName);
			rs = ps.executeQuery();
		String primaryKey =  null;
			while(rs.next()) {
				primaryKey = rs.getString("COLUMN_NAME");
				map.put(tableName, primaryKey);
			}
			
		}
		DBUtils.close(rs, ps, conn);
		return map;
	}
	
	public static void main(String[] args) throws Exception {
		//System.out.println(getTableAttr());
	}
	
	
	
	

}
