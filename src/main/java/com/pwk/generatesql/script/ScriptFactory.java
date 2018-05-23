package com.pwk.generatesql.script;

import com.pwk.generatesql.bean.TableAttr;
import com.pwk.generatesql.utils.Params;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
public class ScriptFactory {
	
	private static final  String TABLE_INSTANTIATION = "EXEC DBMS_CAPTURE_ADM.PREPARE_TABLE_INSTANTIATION(TABLE_NAME => '?');";
	private static final String CHANGE_SET = "EXEC DBMS_CDC_PUBLISH.CREATE_CHANGE_SET(" + 
			" change_set_name => '?'," + 
			" description => 'Change set for CDC ?'," + 
			" change_source_name => 'HOTLOG_SOURCE'," + 
			" stop_on_ddl => 'y'," + 
			" begin_date => sysdate," + 
			" end_date => sysdate+5);";
	private static final String CHANGE_TABLE = "BEGIN\r\n"
			+ " DBMS_CDC_PUBLISH.CREATE_CHANGE_TABLE(\r\n" + 
			"    owner              => '?1',\r\n" + 
			"    change_table_name  => '?2', \r\n" + 
			"    change_set_name    => '?3',\r\n" + 
			"    source_schema      => '?4',\r\n" + 
			"    source_table       => '?5',\r\n" + 
			"    column_type_list   => '?6',\r\n" + 
			"    capture_values     => 'both',\r\n" + 
			"    rs_id              => 'y',\r\n" + 
			"    row_id             => 'n',\r\n" + 
			"    user_id            => 'n',\r\n" + 
			"    timestamp          => 'n',\r\n" + 
			"    object_id          => 'n',\r\n" + 
			"    source_colmap      => 'n',\r\n" + 
			"    target_colmap      => 'y',\r\n" + 
			"    options_string     => 'TABLESPACE ?7');\r\n"
			+ "END;\r\n" + 
			" /\r\n";
	
	private static final String 	ALTER_CHANGE_SET = "EXEC DBMS_CDC_PUBLISH.ALTER_CHANGE_SET(change_set_name => '?',enable_capture => 'Y');";
	private static final String ACCOUNT = Params.getValue("jdbc.username");
	
	private static String tableSpace = "CDC_TBSP";
	
	private static String GRANT_TABLE_SQL="grant all on ? to "+ Params.publisherAccount+";";
	private static String GRANT_TABLE_TO_SUB = "grant select on  ? to "+Params.subscriberAccount+";";
	
	private static String COMMIT_SQL = "commit;";
	
	
	private static List<String> CHANGE_SET_LIST = new ArrayList<String>();
	
	private static BufferedWriter writerBuffer = null;
	
	public static void getSQLFile(Map<String,List<TableAttr>> tableMap, Map<String,String> primaryKey) throws Exception {
			
		
		Set<String> tableSet = tableMap.keySet();
		
		
		for(String tableName : tableSet) {
			String Sql = GRANT_TABLE_SQL.replaceAll("\\?", ACCOUNT+"."+tableName);
			write(Sql);	
		}
		
		write(COMMIT_SQL);	//提交
		
		for(String tableName : tableSet) {
			List<TableAttr> attrList = tableMap.get(tableName);
			
			String startCode = tableName.substring(0,1);
			//*********准备源表(Source Table)*******// 
			String table_initSQL  = TABLE_INSTANTIATION.replaceAll("\\?",ACCOUNT+"."+tableName);
			write(table_initSQL);
			
			//*********创建变更集(Data Set)*******// 
			String DataSetName = "CDC_"+ACCOUNT+"_"+startCode;
			boolean contain = CHANGE_SET_LIST.contains(DataSetName);
			if(!contain) {
				CHANGE_SET_LIST.add(DataSetName);
				String change_set_SQL = CHANGE_SET.replaceAll("\\?", DataSetName);
				write(change_set_SQL);
			}
			
			
			//*********创建变更表*******//
			String change_table_sql = CHANGE_TABLE.replaceAll("\\?1", Params.publisherAccount);
			change_table_sql = change_table_sql.replaceAll("\\?2", "CDC_"+tableName);
			change_table_sql = change_table_sql.replaceAll("\\?3", DataSetName);
			change_table_sql = change_table_sql.replaceAll("\\?4", ACCOUNT);
			change_table_sql = change_table_sql.replaceAll("\\?5", tableName);
			String tableAttrStr = "";
			for(TableAttr attr: attrList) {
				if(attr.getDataType().equals("DATE")) {
					tableAttrStr+=attr.getColumnName()+" "+attr.getDataType()+",";
				}else {
					tableAttrStr+=attr.getColumnName()+" "+attr.getDataType()+"("+attr.getData_length()+"),";
				}
				
			}
			tableAttrStr = tableAttrStr.substring(0,tableAttrStr.length() - 1);
			
			
			change_table_sql = change_table_sql.replaceAll("\\?6", tableAttrStr);
			change_table_sql = change_table_sql.replaceAll("\\?7", tableSpace);
			
			write(change_table_sql);
			
			String alter_change_set = ALTER_CHANGE_SET.replaceAll("\\?", DataSetName);
			write(alter_change_set);	
			
			String grant_table_to_subscrib = GRANT_TABLE_TO_SUB.replaceAll("\\?", "CDC_"+tableName);//授权查询角色给订阅者
			write(grant_table_to_subscrib);	
			
		}
		
	}
	
	
	
	private static final String create_subscription = "BEGIN\r\n" + 
			"    dbms_cdc_subscribe.create_subscription(\r\n" + 
			"    change_set_name=>'?1',\r\n" + 
			"    description=>'cdc ?2', \r\n" + 
			"    subscription_name=>'?3');\r\n" + 
			"    END;\r\n" + 
			"    /";
	
	private static final String subscribe = "BEGIN\r\n" + 
			" dbms_cdc_subscribe.subscribe(\r\n" + 
			" subscription_name=>'?1', \r\n" + 
			" source_schema=>'?2', \r\n" + 
			" source_table=>'?3',\r\n" + 
			" column_list=>'?4',\r\n" + 
			" subscriber_view=>'?5');\r\n" + 
			" END;\r\n" + 
			" /";
	
	private static final String activate_subscription = "EXEC dbms_cdc_subscribe.activate_subscription(subscription_name=>'?');";
	
	
	public static void getSubscriberSQLInfo(Map<String,List<TableAttr>> tableMap,Map<String,String> primaryKey) throws Exception {
		
		Set<String> tableSet = tableMap.keySet();
		
		
		for(String  tableName: tableSet) {
			String startCode = tableName.substring(0,1);
			List<TableAttr> attrList = tableMap.get(tableName);
			//*********创建订阅集*******// 
			String DataSetName = "CDC_"+ACCOUNT+"_"+startCode;
			String create_subscription_sql = create_subscription.replaceAll("\\?1", DataSetName);
			create_subscription_sql = create_subscription_sql.replaceAll("\\?2", "cdc "+ ACCOUNT+" subx");
			create_subscription_sql = create_subscription_sql.replaceAll("\\?3",tableName+"_SUB");
			write(create_subscription_sql);
			
			
			String subscribe_sql = subscribe.replaceAll("\\?1",tableName+"_SUB");
			subscribe_sql = subscribe_sql.replaceAll("\\?2", ACCOUNT );			
			subscribe_sql = subscribe_sql.replaceAll("\\?3", tableName );
			String attr_sql = "";
			for(TableAttr attr: attrList) {
				attr_sql +=attr.getColumnName() +",";
			}
			attr_sql = attr_sql.substring(0,attr_sql.length() - 1);
			
			subscribe_sql = subscribe_sql.replaceAll("\\?4", attr_sql );			
			subscribe_sql = subscribe_sql.replaceAll("\\?5", tableName+"_TEMP" );			
			
			write(subscribe_sql);
			
			String activate_subscription_sql =  activate_subscription.replaceAll("\\?",tableName+"_SUB");
			write(activate_subscription_sql);
			
		}
		
		
	}
	
	
	
	public static void getPreSQLInfo() throws Exception{
		writerBuffer = new BufferedWriter(new FileWriter(new File("/opt/sql.txt")));
		InputStream inputStram  = ScriptFactory.class.getResourceAsStream("/init.sql");
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStram));
		String Info = "-- Start Auto Compile SQL -- \r\n"
				   + " --       GoldenBridge     --\r\n";
		writerBuffer.write(Info);
		while( Info != null) {
			Info = bufferedReader.readLine();
			if(Info != null)
			writerBuffer.write(Info+"\r\n");
		}
		bufferedReader.close();
		
	}
	
	
	public static void close() throws Exception{
		writerBuffer.flush();
		writerBuffer.close();
	}
	
	
	public static void  write(String info) throws Exception {
		writerBuffer.write(info +"\r\n");
	}
	
	public static void getDeleteSQLFile() {
		String sql1="cdc_change_tables";//查询改变的表
		String sql2="all_change_sets";//查询改变的集合
		String sql3="dba_capture_prepared_tables";//查询实例化的表
		
		//cdc_subscribers$ 订阅者表信息
	}
	
	
	
	public static void main(String[] args) throws Exception{
	 String result =  TABLE_INSTANTIATION.replaceAll("\\?", "TEST");
	 System.out.println(result);
	}
	
	

}
