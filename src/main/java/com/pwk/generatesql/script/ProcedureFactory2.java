/**
 * 
 */
package com.pwk.generatesql.script;

import com.pwk.generatesql.bean.TableAttr;
import com.pwk.generatesql.utils.Params;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
*@author		create by pengweikang
*@date		2018年3月29日--下午4:20:15
*@problem
*@answer
*@action
*/

public class ProcedureFactory2 {

	public static  BufferedWriter bufferedWriter  =null;
	
	public static String dbLink = Params.getValue("dblink");
	

	
	
	private static final String ACCOUNT = Params.getValue("jdbc.username");
	
	
	
	public static void getProcedureSql(Map<String,List<TableAttr>> tableMap,Map<String,String> primaryKey) throws Exception{
		bufferedWriter = new BufferedWriter(new FileWriter(new File("/opt/sql_procedure.txt")));
		
		Set<String> tableSet = tableMap.keySet();
		
		
		for(String tableName : tableSet) {
			List<TableAttr> tableAttrs = tableMap.get(tableName);
			
			String startCode = tableName.substring(0,1);
			String DataSetName = "CDC_"+ACCOUNT+"_"+startCode;
			StringBuffer strBuffer = new StringBuffer();
			strBuffer.append( "create or replace procedure CDC_"+ACCOUNT+"_"+tableName+" is\r\n");
			strBuffer.append("type t_cur is REF CURSOR;\r\n");
			strBuffer.append("errorException exception;\r\n");
			String countList = "";
			String column_attr_List = "";
			String update_column_List = "";
			String v_column_attr_List = "";
			String column_list = "";
			String newOldUpdateColumn = "";
			String setupdateSql= "";
			String updateWhereSql = "";
			String updateWhereSqlStr = "";
			for(TableAttr attr :  tableAttrs) {
				
				column_attr_List += "V_"+attr.getColumnName()+" "+tableName+"_TEMP."+attr.getColumnName()+"%type;\r\n";	
				update_column_List += "TP_"+attr.getColumnName()+" "+tableName+"_TEMP."+attr.getColumnName()+"%type;\r\n";	
				countList += "t."+attr.getColumnName()+",";
				v_column_attr_List += "V_"+attr.getColumnName()+",";
				column_list += attr.getColumnName()+",";
				newOldUpdateColumn += "            TP_"+attr.getColumnName()+" := V_"+attr.getColumnName()+";\r\n";	
				//
				//if  names is not null then 
		       // select_name := select_name || ',name= ''' || names || '''';
			    //end if;
				
				setupdateSql +="if V_"+attr.getColumnName()+" is not null  then \r\n" + 
						"        update_sql := update_sql || ',"+attr.getColumnName()+"=''' || V_"+attr.getColumnName()+" || '''';\r\n" + 
						"    end if;\r\n";
					
				//setupdateSql += attr.getColumnName()+"=V_"+attr.getColumnName()+",";
				updateWhereSql += attr.getColumnName()+"=TP_"+attr.getColumnName()+",";
				
			}
			
			String updateOrDeleteWhereSql = "";
			if(primaryKey.get(tableName)!= null) {
				updateOrDeleteWhereSql = primaryKey.get(tableName)+"=TP_"+primaryKey.get(tableName);
				updateWhereSqlStr = primaryKey.get(tableName)+"=TP_"+primaryKey.get(tableName);
			}else {
				updateOrDeleteWhereSql = updateWhereSql.substring(0,updateWhereSql.length() - 1);
			}
			
			
			column_list = column_list.substring(0,column_list.length() - 1);
			//setupdateSql = setupdateSql.substring(0,setupdateSql.length() - 1);
			countList = countList.substring(0,countList.length() - 1);
			v_column_attr_List = v_column_attr_List.substring(0,v_column_attr_List.length() - 1);
			
			strBuffer.append(column_attr_List);
			strBuffer.append(" v_operation$ "+tableName+"_TEMP.operation$%type;\r\n");
			strBuffer.append(update_column_List);			
			strBuffer.append("update_sql varchar2(4000);\r\n");
			strBuffer.append("v_sql_str varchar2(4000);\r\n");
			strBuffer.append("usrs t_cur;\r\n");
			strBuffer.append("begin\r\n" + 
							"    begin\r\n" + 
							"      dbms_cdc_subscribe.purge_window(subscription_name=>'"+tableName+"_SUB"+"');\r\n" + 
							"    end;\r\n" + 
							"    begin\r\n" + 
							"      dbms_cdc_subscribe.extend_window(subscription_name=>'"+tableName+"_SUB"+"');\r\n" + 
							"    end;");
			strBuffer.append("    open usrs for select t.operation$,"+countList+" from "+tableName+"_TEMP t order by t.commit_timestamp$ asc,t.operation$ desc; \r\n");
			strBuffer.append("    loop\r\n" + 
			"        fetch usrs into v_operation$,"+v_column_attr_List+";\r\n" + 
			"        exit when usrs%notfound;\r\n");
			
			strBuffer.append("        if v_operation$ = 'I' then --执行新增\r\n" + 
			"            insert into "+tableName+"@"+dbLink+"("+column_list+") values("+v_column_attr_List+");\r\n" + 
			"        elsif v_operation$ = 'UO' then --获取更新信息\r\n" + 
						newOldUpdateColumn+ 
			"        elsif v_operation$ = 'UN' then --执行更新\r\n" +
			setupdateSql+//获取更新信息
						" update_sql := substr(update_sql,2);   \r\n"+
		//	"            update "+tableName+"@"+dbLink+" set "+setupdateSql+" where "+updateOrDeleteWhereSql+";\r\n" + 
			" 			v_sql_str := 'update "+tableName+"@"+dbLink+" SET ' || update_sql ||' where "+updateOrDeleteWhereSql+"';	\r\n"+
			"			execute immediate v_sql_str;\r\n"+
			"        elsif v_operation$ = 'D' then --执行删除\r\n" + 
			"            delete from "+tableName+"@"+dbLink+" where "+updateOrDeleteWhereSql+";\r\n" + 
			"        end if;\r\n");
			strBuffer.append("    end loop;\r\n" + 
			"    close usrs;\r\n" + 
			"    commit;\r\n" + 
			"    exception  \r\n" + 
			"         when errorException then\r\n" + 
			"            dbms_output.put_line('数据异常');\r\n" + 
			"end CDC_"+ACCOUNT+"_"+tableName+";\r\n\r\n");
			bufferedWriter.write(strBuffer.toString());	
		}		
		close();
	}
	
	
	public static void close() throws IOException {
		bufferedWriter.flush();
		bufferedWriter.close();
	}
	
}
