dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/tmpsepatest;
CREATE TABLE separator_test 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = "|","quoteChar"="\"","escapeChar"="
") 
STORED AS TEXTFILE
LOCATION 'file:${system:test.tmp.dir}/tmpsepatest'
AS
SELECT * FROM src where key = 100 limit 1; 
dfs -cat ${system:test.tmp.dir}/tmpsepatest/000000_0;
drop table separator_test;
