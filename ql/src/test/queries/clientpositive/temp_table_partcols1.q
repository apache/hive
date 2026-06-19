--! qt:dataset:src
set metastore.integral.jdo.pushdown=true;

create temporary table test1_n15_temp(col1 string) partitioned by (partitionid int);
insert overwrite table test1_n15_temp partition (partitionid=1)
  select key from src tablesample (10 rows);

FROM (
  FROM test1_n15_temp
  SELECT partitionid, 111 as col2, 222 as col3, 333 as col4
  WHERE partitionid = 1
  DISTRIBUTE BY partitionid
  SORT BY partitionid
) b

SELECT TRANSFORM(
b.partitionid,b.col2,b.col3,b.col4
)

USING 'cat' as (a,b,c,d);