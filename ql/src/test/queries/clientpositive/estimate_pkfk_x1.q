set hive.query.results.cache.enabled=false;
set hive.explain.user=true;

drop table if exists default.rx0;
drop table if exists default.sr0;

create table rx0 (r_reason_id string, r_reason_sk bigint);
create table sr0 (sr_reason_sk bigint);

insert into rx0 values ('AAAAAAAAAAAAAAAA',1),('AAAAAAAAGEAAAAAA',70);

insert into sr0 values (NULL),(1),
(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),(1),(70),
(70);


desc formatted sr0 sr_reason_sk;

insert into sr0 select a.* from sr0 a,sr0 b;
-- |sr0| ~ 5112


desc formatted sr0 sr_reason_sk;

analyze table sr0 compute statistics for columns;

desc formatted sr0 sr_reason_sk;

explain analyze select 1
from default.sr0  store_returns , default.rx0 reason
            where sr_reason_sk = r_reason_sk
              and r_reason_id = 'AAAAAAAAAAAAAAAA'
;

