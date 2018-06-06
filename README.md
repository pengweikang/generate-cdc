# generate-cdc
# 文件目录

   ├─ src
    │  ├─ bean
    │  ├─ core
    │  ├─ db
    │  ├─ script
    │  ├─ utils
    │  └─ App.java
    ├─ resources
    │  ├─ init.sql
    │  └─ jdbc.properties
    └─ pom.xml


# 程序主入口
com.pwk.generatesql.App.java
```java

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

```