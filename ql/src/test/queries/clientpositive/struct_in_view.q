drop table testreserved;

create table testreserved (data struct<`end`:string, id: string>);

create view testreservedview as select data.`end` as data_end, data.id as data_id from testreserved;

describe extended testreservedview;

select data.`end` from testreserved;

drop view testreservedview;

drop table testreserved;

create table s (default struct<src:struct<`end`:struct<key:string>, id: string>, id: string>);

create view vs1 as select default.src.`end`.key from s;

describe extended vs1;

create view vs2 as select default.src.`end` from s;

describe extended vs2;

drop view vs1;

drop view vs2;

create view v as select named_struct('key', 1).key from src limit 1;

desc extended v;

select * from v;

set hive.cbo.returnpath.hiveop=true;
select * from v;
set hive.cbo.returnpath.hiveop=false;

drop view v;

create view v as select named_struct('end', 1).`end` from src limit 1;

desc extended v;

select * from v;

drop view v;

