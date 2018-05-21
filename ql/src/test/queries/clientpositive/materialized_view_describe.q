set hive.vectorized.execution.enabled=false;
create table cmv_basetable_n8 (a int, b varchar(256), c decimal(10,2));

insert into cmv_basetable_n8 values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view cmv_mat_view_n8
comment 'this is the first view'
tblproperties ('key'='foo') as select a, c from cmv_basetable_n8;

describe cmv_mat_view_n8;

describe extended cmv_mat_view_n8;

describe formatted cmv_mat_view_n8;

show tblproperties cmv_mat_view_n8;

select a, c from cmv_mat_view_n8;

drop materialized view cmv_mat_view_n8;

create materialized view cmv_mat_view2_n3
comment 'this is the second view'
stored as textfile
tblproperties ('key'='alice','key2'='bob') as select a from cmv_basetable_n8;

describe formatted cmv_mat_view2_n3;

select a from cmv_mat_view2_n3;

drop materialized view cmv_mat_view2_n3;

create materialized view cmv_mat_view3_n0
comment 'this is the third view'
row format
  delimited fields terminated by '\t'
as select * from cmv_basetable_n8;

describe formatted cmv_mat_view3_n0;

select a, b, c from cmv_mat_view3_n0;

select distinct a from cmv_mat_view3_n0;

drop materialized view cmv_mat_view3_n0;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/t;

create materialized view cmv_mat_view4_n0
comment 'this is the last view'
stored as textfile
location '${system:test.tmp.dir}/t'
as select a from cmv_basetable_n8;

describe formatted cmv_mat_view4_n0;

select a from cmv_mat_view4_n0;

drop materialized view cmv_mat_view4_n0;
