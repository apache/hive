Hive2.2.0 修改记录  

| 序号 | 项目 | 类 | 方法 | 行号 | 修改功能说明 | 修改代码 |  
| --- | --- | --- | --- | --- | --- | --- |  
|1|hive-exec|org.apache.hadoop.hive.ql.exec.FetchOperator.java|getInputFormat|233,402|检测到使用的InputFormat是Hbase时，初始化HiveHbaseTableInputFormat时将Operator对象传递进去|-|
