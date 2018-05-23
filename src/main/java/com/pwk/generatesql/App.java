package com.pwk.generatesql;

import com.pwk.generatesql.bean.TableAttr;
import com.pwk.generatesql.core.TableAttrFactory;
import com.pwk.generatesql.core.TableFactory;
import com.pwk.generatesql.script.DeleteAllFactory;
import com.pwk.generatesql.script.ProcedureFactory2;
import com.pwk.generatesql.script.ScriptFactory;

import java.util.List;
import java.util.Map;


/**
 *@author		create by pengweikang
 *@date		2018年3月29日--下午4:20:15
 *@problem
 *@answer
 *@action
 */
public class App {



    public static void main(String[] args) throws Exception{
        List<String> tableList = TableFactory.getTableList();
        Map<String, List<TableAttr>> mapTable = TableAttrFactory.getTableAttr(tableList);
        Map<String,String> primaryKeyMap = TableAttrFactory.getTablePrimaryKey(tableList);
        ScriptFactory.getPreSQLInfo();//获取前置数据库配置信息配置
        ScriptFactory.getSQLFile(mapTable, primaryKeyMap); //自动生成CDC发布订者本文件
        ScriptFactory.getSubscriberSQLInfo(mapTable, primaryKeyMap); //自动生成CDC订阅脚本文件
        ProcedureFactory2.getProcedureSql(mapTable, primaryKeyMap);//自动生成数据同步存储过程
        DeleteAllFactory.generatorSql(mapTable);//自动生成删除CDC环境脚本
        ScriptFactory.close();
    }

}
