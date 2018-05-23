package com.pwk.generatesql.bean;
/**
*@author		create by pengweikang
*@date		2018年3月29日--下午4:20:15
*@problem
*@answer
*@action
*/
public class TableAttr {
	
	private String tableName;
	private String ColumnName;
	private String dataType;
	private String nullable;
	private String dataDefault;
	private String ColumnId;
	private String comments;
	private int data_length;
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getColumnName() {
		return ColumnName;
	}
	public void setColumnName(String columnName) {
		ColumnName = columnName;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getNullable() {
		return nullable;
	}
	public void setNullable(String nullable) {
		this.nullable = nullable;
	}
	public String getDataDefault() {
		return dataDefault;
	}
	public void setDataDefault(String dataDefault) {
		this.dataDefault = dataDefault;
	}
	public String getColumnId() {
		return ColumnId;
	}
	public void setColumnId(String columnId) {
		ColumnId = columnId;
	}
	public String getComments() {
		return comments;
	}
	public void setComments(String comments) {
		this.comments = comments;
	}
	public int getData_length() {
		return data_length;
	}
	public void setData_length(int data_length) {
		this.data_length = data_length;
	}
	
	
	
	

}
