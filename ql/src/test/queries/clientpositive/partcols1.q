--! qt:dataset:src

create table test1_n15(col1 string) partitioned by (partitionId int);
insert overwrite table test1_n15 partition (partitionId=1)
  select key from src tablesample (10 rows);

 FROM (
 FROM test1_n15
 SELECT partitionId, 111 as col2, 222 as col3, 333 as col4
 WHERE partitionId = 1
 DISTRIBUTE BY partitionId
 SORT BY partitionId
 ) b

SELECT TRANSFORM(
 b.partitionId,b.col2,b.col3,b.col4
 )

 USING 'cat' as (a,b,c,d);