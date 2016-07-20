create table cmv_basetable (a int, b varchar(256), c decimal(10,2));

insert into cmv_basetable values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view cmv_mat_view
comment 'this is the first view'
tblproperties ('key'='foo') as select a, c from cmv_basetable;

describe cmv_mat_view;

describe extended cmv_mat_view;

describe formatted cmv_mat_view;

show tblproperties cmv_mat_view;

select a, c from cmv_mat_view;

drop materialized view cmv_mat_view;

create materialized view cmv_mat_view2
comment 'this is the second view'
stored as textfile
tblproperties ('key'='alice','key2'='bob') as select a from cmv_basetable;

describe formatted cmv_mat_view2;

select a from cmv_mat_view2;

drop materialized view cmv_mat_view2;

create materialized view cmv_mat_view3
comment 'this is the third view'
row format
  delimited fields terminated by '\t'
as select * from cmv_basetable;

describe formatted cmv_mat_view3;

select a, b, c from cmv_mat_view3;

select distinct a from cmv_mat_view3;

drop materialized view cmv_mat_view3;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/t;

create materialized view cmv_mat_view4
comment 'this is the last view'
stored as textfile
location '${system:test.tmp.dir}/t'
as select a from cmv_basetable;

describe formatted cmv_mat_view4;

select a from cmv_mat_view4;

drop materialized view cmv_mat_view4;
