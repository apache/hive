DROP TABLE users;

CREATE EXTERNAL TABLE users(key string, state string, country string, country_id int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = "info:state,info:country,info:country_id"
)
TBLPROPERTIES ("external.table.purge" = "true");

desc formatted users;

explain INSERT OVERWRITE TABLE users SELECT 'user1', 'IA', 'USA', 0 FROM src;

INSERT OVERWRITE TABLE users SELECT 'user1', 'IA', 'USA', 0 FROM src;

desc formatted users;

select count(*) from users;

set hive.compute.query.using.stats=true;

select count(*) from users;

INSERT into TABLE users SELECT 'user2', 'IA', 'USA', 0 FROM src;

desc formatted users;

select count(*) from users;

analyze table users compute statistics;

desc formatted users;

explain select count(*) from users;

select count(*) from users;

INSERT into TABLE users SELECT 'user3', 'IA', 'USA', 0 FROM src;

desc formatted users;

explain select count(*) from users;

select count(*) from users;

