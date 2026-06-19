--! qt:dataset:src
set hive.multi.insert.move.tasks.share.dependencies=true;
set hive.stats.dbclass=fs;
-- SORT_QUERY_RESULTS

create table src_multi1_n4 like src;
create table src_multi2_n5 like src;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

select * from src_multi1_n4;
select * from src_multi2_n5;


set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

select * from src_multi1_n4;
select * from src_multi2_n5;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=true;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

select * from src_multi1_n4;
select * from src_multi2_n5;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

select * from src_multi1_n4;
select * from src_multi2_n5;



set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10 group by key, value
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20 group by key, value;

from src
insert overwrite table src_multi1_n4 select * where key < 10 group by key, value
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20 group by key, value;

select * from src_multi1_n4;
select * from src_multi2_n5;


set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=true;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10 group by key, value
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20 group by key, value;

from src
insert overwrite table src_multi1_n4 select * where key < 10 group by key, value
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20 group by key, value;

select * from src_multi1_n4;
select * from src_multi2_n5;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10 group by key, value
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20 group by key, value;

from src
insert overwrite table src_multi1_n4 select * where key < 10 group by key, value
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20 group by key, value;

select * from src_multi1_n4;
select * from src_multi2_n5;


set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10 group by key, value
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20 group by key, value;

from src
insert overwrite table src_multi1_n4 select * where key < 10 group by key, value
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20 group by key, value;

select * from src_multi1_n4;
select * from src_multi2_n5;




set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

explain
from (select * from src  union all select * from src) s
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

from (select * from src  union all select * from src) s
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

select * from src_multi1_n4;
select * from src_multi2_n5;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;

explain
from (select * from src  union all select * from src) s
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

from (select * from src  union all select * from src) s
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

select * from src_multi1_n4;
select * from src_multi2_n5;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=true;

explain
from (select * from src  union all select * from src) s
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

from (select * from src  union all select * from src) s
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

select * from src_multi1_n4;
select * from src_multi2_n5;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

explain
from (select * from src  union all select * from src) s
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

from (select * from src  union all select * from src) s
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20;

select * from src_multi1_n4;
select * from src_multi2_n5;



set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hive_test/multiins_local/temp;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

explain
from src 
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/0' select * where key = 0
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key = 2
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/4' select * where key = 4;

from src 
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/0' select * where key = 0
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key = 2
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/4' select * where key = 4;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;

explain
from src 
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/0' select * where key = 0
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key = 2
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/4' select * where key = 4;

from src 
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/0' select * where key = 0
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key = 2
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/4' select * where key = 4;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=true;


explain
from src 
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/0' select * where key = 0
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key = 2
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/4' select * where key = 4;

from src 
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/0' select * where key = 0
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key = 2
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/4' select * where key = 4;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

explain
from src 
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/0' select * where key = 0
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key = 2
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/4' select * where key = 4;

from src 
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/0' select * where key = 0
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key = 2
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/4' select * where key = 4;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

explain
from src
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

from src
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;

explain
from src
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

from src
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=true;

explain
from src
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

from src
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

explain
from src
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

from src
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

select * from src_multi1_n4;
select * from src_multi2_n5;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=true;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

select * from src_multi1_n4;
select * from src_multi2_n5;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

select * from src_multi1_n4;
select * from src_multi2_n5;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

explain
from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

from src
insert overwrite table src_multi1_n4 select * where key < 10
insert overwrite table src_multi2_n5 select * where key > 10 and key < 20
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/1' select * where key < 10 group by key, value cluster by key
insert overwrite local directory '${system:test.tmp.dir}/hive_test/multiins_local/2' select * where key > 10 and key < 20 group by key, value cluster by value;

select * from src_multi1_n4;
select * from src_multi2_n5;

dfs -ls ${system:test.tmp.dir}/hive_test/multiins_local;
dfs -rmr ${system:test.tmp.dir}/hive_test/multiins_local;
