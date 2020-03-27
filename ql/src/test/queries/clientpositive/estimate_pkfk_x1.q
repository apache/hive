set hive.query.results.cache.enabled=false;
set hive.explain.user=true;

drop table if exists default.rx0;
drop table if exists default.sr0;

create table rx0 (r_reason_id string, r_reason_sk bigint);
create table sr0 (sr_reason_sk bigint);

insert into rx0 values ('AAAAAAAAAAAAAAAA',1),('AAAAAAAAGEAAAAAA',70);

insert into sr0 values (NULL),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),
(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),
(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
(41),(42),(43),(44),(45),(46),(47),(48),(49),(50),(51),(52),(53),(54),(55),
(56),(57),(58),(59),(60),(61),(62),(63),(64),(65),(66),(67),(68),(69),(70);


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

