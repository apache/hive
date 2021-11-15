SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table testdatatypetable
        (c1 int, c2 boolean, c3 double, c4 string,
         c5 array<int>, c6 map<int,string>, c7 map<string,string>,
         c8 struct<r:string,s:int,t:double>,
         c9 tinyint, c10 smallint, c11 float, c12 bigint,
         c13 array<array<string>>,
         c14 map<int, map<int,int>>,
         c15 struct<r:int,s:struct<a:int,b:string>>,
         c16 array<struct<m:map<string,string>,n:int>>,
         c17 timestamp, 
         c18 decimal(16,7), 
         c19 binary, 
         c20 date,
         c21 varchar(20),
         c22 char(15),
         c23 binary
        ) partitioned by (dt STRING);

load data local inpath '../../data/files/datatypes.txt' into table testdatatypetable PARTITION (dt='20090619');

explain vectorization detail
select
c1, c2, c3, c4, c5 as a, c6, c7, c8, c9, c10, c11, c12, c1*2,
sentences(null, null, null) as b, c17, c18, c20, c21, c22, c23 from testdatatypetable limit 1;