--! qt:dataset:src
drop table testreserved;

create table testreserved (data struct<`end`:string, id: string>);

create view testreservedview as select data.`end` as data_end, data.id as data_id from testreserved;

describe extended testreservedview;

select data.`end` from testreserved;

drop view testreservedview;

drop table testreserved;

create table s_n1 (default struct<src:struct<`end`:struct<key:string>, id: string>, id: string>);

create view vs1 as select default.src.`end`.key from s_n1;

describe extended vs1;

create view vs2 as select default.src.`end` from s_n1;

describe extended vs2;

drop view vs1;

drop view vs2;

create view v_n3 as select named_struct('key', 1).key from src limit 1;

desc extended v_n3;

select * from v_n3;

set hive.cbo.returnpath.hiveop=true;
select * from v_n3;
set hive.cbo.returnpath.hiveop=false;

drop view v_n3;

create view v_n3 as select named_struct('end', 1).`end` from src limit 1;

desc extended v_n3;

select * from v_n3;

drop view v_n3;

