set hive.metastore.partition.inherit.table.properties="";
create table mytbl_n2 (c1 tinyint) partitioned by (c2 string) tblproperties ('a'='myval','b'='yourval','c'='noval');
alter table mytbl_n2 add partition (c2 = 'v1');
describe formatted mytbl_n2 partition (c2='v1');
