create database vt;

create view vt.v as select * from srcpart;
describe formatted vt.v;

-- modifying definition of unpartitioned view
create or replace view vt.v partitioned on (ds, hr) as select * from srcpart;
alter view vt.v add partition (ds='2008-04-08',hr='11');
alter view vt.v add partition (ds='2008-04-08',hr='12');
select * from vt.v where value='val_409' and ds='2008-04-08' and hr='11';
describe formatted vt.v;
show partitions vt.v;

alter view vt.v drop partition (ds='2008-04-08',hr='11');
alter view vt.v drop partition (ds='2008-04-08',hr='12');
show partitions vt.v;

-- altering partitioned view 1
create or replace view vt.v partitioned on (ds, hr) as select value, ds, hr from srcpart;
select * from vt.v where value='val_409' and ds='2008-04-08' and hr='11';
describe formatted vt.v;
show partitions vt.v;

-- altering partitioned view 2
create or replace view vt.v partitioned on (ds, hr) as select key, value, ds, hr from srcpart;
select * from vt.v where value='val_409' and ds='2008-04-08' and hr='11';
describe formatted vt.v;
show partitions vt.v;
drop view vt.v;

-- updating to fix view with invalid definition
create table srcpart_temp like srcpart;
create view vt.v partitioned on (ds, hr) as select * from srcpart_temp;
drop table srcpart_temp; -- vt.v is now invalid
create or replace view vt.v partitioned on (ds, hr) as select * from srcpart;
describe formatted vt.v;
drop view vt.v;

drop database vt;