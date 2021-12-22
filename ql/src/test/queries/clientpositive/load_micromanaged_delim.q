set hive.support.concurrency=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


dfs -mkdir ${system:test.tmp.dir}/delim_table;
dfs -mkdir ${system:test.tmp.dir}/delim_table_ext;
dfs -mkdir ${system:test.tmp.dir}/delim_table_trans;
dfs -cp ${system:hive.root}/data/files/table1_delim.txt ${system:test.tmp.dir}/delim_table/;
dfs -cp ${system:hive.root}/data/files/table1_delim.txt ${system:test.tmp.dir}/delim_table_ext/;
dfs -cp ${system:hive.root}/data/files/table1_delim.txt ${system:test.tmp.dir}/delim_table_trans/;

-- Checking that MicroManged and External tables have the same behaviour with delimited input files
-- External table
CREATE EXTERNAL TABLE delim_table_ext(id INT, name STRING, safety INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '${system:test.tmp.dir}/delim_table_ext/';
describe formatted delim_table_ext;
SELECT * FROM delim_table_ext;

-- MicroManaged insert_only table
CREATE TABLE delim_table_micro(id INT, name STRING, safety INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE TBLPROPERTIES('transactional'='true', "transactional_properties"="insert_only");
LOAD DATA INPATH '${system:test.tmp.dir}/delim_table/table1_delim.txt' OVERWRITE INTO TABLE delim_table_micro;
describe formatted delim_table_micro;
SELECT * FROM delim_table_micro;

-- Same as above with different syntax
CREATE TRANSACTIONAL TABLE delim_table_trans(id INT, name STRING, safety INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
LOAD DATA INPATH '${system:test.tmp.dir}/delim_table_trans/table1_delim.txt' OVERWRITE INTO TABLE delim_table_trans;
describe formatted delim_table_trans;
SELECT * FROM delim_table_trans;
