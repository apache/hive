set hive.map.aggr=false;

set hive.explain.user=true;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING);

insert overwrite table tab partition (ds='2008-04-08')
select key,value from src;

explain analyze 
select s1.key as key, s1.value as value from tab s1 join tab s3 on s1.key=s3.key;

explain analyze 
select s1.key as key, s1.value as value from tab s1 join tab s3 on s1.key=s3.key join tab s2 on s1.value=s2.value;

