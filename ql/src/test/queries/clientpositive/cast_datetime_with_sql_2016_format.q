--non-vectorized
set hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=more;

create table timestamp1 (t timestamp) stored as parquet;
insert into timestamp1 values
("2020-02-03"),
("1969-12-31 23:59:59.999999999")
;
from timestamp1 select cast (t as string      format "yyyy hh24...PM ff");
from timestamp1 select cast (t as char(11)    format "yyyy hh24...PM ff"); -- will be truncated
from timestamp1 select cast (t as varchar(11) format "yyyy hh24...PM ff"); -- will be truncated

create table dates (d date) stored as parquet;
insert into dates values
("2020-02-03"),
("1969-12-31")
;
from dates select cast (d as string      format "yyyy mm dd , hh24 mi ss ff9");
from dates select cast (d as char(10)    format "yyyy mm dd , hh24 mi ss ff9"); -- will be truncated
from dates select cast (d as varchar(10) format "yyyy mm dd , hh24 mi ss ff9"); -- will be truncated

create table strings  (s string)      stored as parquet;
create table varchars (s varchar(11)) stored as parquet;
create table chars    (s char(11))    stored as parquet;
insert into strings values
("20 / 2 / 3"),
("1969 12 31")
;
insert into varchars select * from strings;
insert into chars select * from strings;

from strings    select cast (s as timestamp format "yyyy.mm.dd");
from strings    select cast (s as date      format "yyyy.mm.dd");
from varchars   select cast (s as timestamp format "yyyy.mm.dd");
from varchars   select cast (s as date      format "yyyy.mm.dd");
from chars      select cast (s as timestamp format "yyyy.mm.dd");
from chars      select cast (s as date      format "yyyy.mm.dd");

--output null when input can't be parsed
select cast ("2015-05-15 12::00" as timestamp format "yyyy-mm-dd hh:mi:ss");
select cast ("2015-05-15 12::00" as date format "yyyy-mm-dd hh:mi:ss");

--correct descriptions
explain from strings    select cast (s as timestamp   format "yyy.mm.dd");
explain from strings    select cast (s as date        format "yyy.mm.dd");
explain from timestamp1 select cast (t as string      format "yyyy");
explain from timestamp1 select cast (t as varchar(12) format "yyyy");


--vectorized
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

from timestamp1 select cast (t as string      format "yyyy");
from dates      select cast (d as string      format "yyyy");
from timestamp1 select cast (t as varchar(11) format "yyyy");
from dates      select cast (d as varchar(11) format "yyyy");
from timestamp1 select cast (t as char(11)    format "yyyy");
from dates      select cast (d as char(11)    format "yyyy");
from strings    select cast (s as timestamp   format "yyyy.mm.dd");
from varchars   select cast (s as timestamp   format "yyyy.mm.dd");
from chars      select cast (s as timestamp   format "yyyy.mm.dd");
from strings    select cast (s as date        format "yyyy.mm.dd");
from varchars   select cast (s as date        format "yyyy.mm.dd");
from chars      select cast (s as date        format "yyyy.mm.dd");
