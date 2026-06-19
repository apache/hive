
CREATE TABLE table1(
   t1_col1 bigint);

 CREATE TABLE table2(
   t2_col1 bigint,
   t2_col2 int)
 PARTITIONED BY (
   t2_col3 date);

insert into table1 values(1);
insert into table2 values("1","1","1");

set hive.support.quoted.identifiers=none;

create external table ext_table STORED AS ORC tblproperties('compression'='snappy','external.table.purge'='true') as
SELECT a.* ,d.`(t2_col1|t2_col3)?+.+`
FROM table1 a
LEFT JOIN (SELECT * FROM table2 where t2_col3 like '2021-01-%') d
on a.t1_col1 = d.t2_col1;
