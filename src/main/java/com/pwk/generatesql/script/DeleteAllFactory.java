/**
 * 
 */
package com.pwk.generatesql.script;
/**
*@author		create by pengweikang
*@date		2018年3月30日--下午4:11:58
*@problem
*@answer
*@action
*/

import com.pwk.generatesql.bean.TableAttr;
import com.pwk.generatesql.utils.Params;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class DeleteAllFactory {
	
	public static BufferedWriter writerBuffer = null;
	private static final String ACCOUNT = Params.getValue("jdbc.username");
	private static List<String> CHANGE_SET_LIST = new ArrayList<String>();
	
	
	public static void generatorSql(Map<String, List<TableAttr>> mapTable) throws Exception {
		writerBuffer = new BufferedWriter(new FileWriter(new File("/opt/delete.txt")));
		
		StringBuffer strBuffer = new StringBuffer();
		strBuffer.append("conn "+Params.subscriberAccount +"/"+Params.subscriberPasswd+"\r\n");
		Set<String> keySet = mapTable.keySet();
		
		String abort_table_instantiation = "";
		String drop_change_table = "";
		String 	drop_change_set = "";
		
		for(String tableName : keySet) {
			strBuffer.append("exec dbms_cdc_subscribe.drop_subscription('"+tableName+"_SUB');\r\n");
			abort_table_instantiation += "exec dbms_capture_adm.abort_table_instantiation('"+ACCOUNT+"."+tableName+"');\r\n";
			drop_change_table+= "exec dbms_cdc_publish.drop_change_table('"+Params.publisherAccount+"', 'CDC_"+tableName+"', 'Y');\r\n";
			String startCode = tableName.substring(0,1);
			String DataSetName = "CDC_"+ACCOUNT+"_"+startCode;
			boolean contain = CHANGE_SET_LIST.contains(DataSetName);
			if(!contain) {
				CHANGE_SET_LIST.add(DataSetName);
				drop_change_set += "exec dbms_cdc_publish.drop_change_set('"+DataSetName+"');\r\n";
			}
			
			
		}
		
		strBuffer.append("conn / as sysdba\r\n");
		
		strBuffer.append(abort_table_instantiation);
		strBuffer.append(drop_change_table);
		strBuffer.append(drop_change_set);
		writerBuffer.write(strBuffer.toString());
		
		writerBuffer.flush();
		writerBuffer.close();
		
	}

	
	
	
}
