
create table tx_n3(a int,u int);
insert into tx_n3 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(10,10);

create table px_n0(a int,p int);
insert into px_n0 values (2,2),(3,3),(5,5),(7,7),(11,11);

set hive.explain.user=true;
set hive.query.reexecution.enabled=true;
set hive.query.reexecution.always.collect.operator.stats=true;
set hive.query.reexecution.strategies=overlay,reoptimize,recompile_without_cbo;
set hive.query.reexecution.stats.persist.scope=hiveserver;

-- join output estimate is underestimated: 1 row
explain
select sum(u*p) from tx_n3 join px_n0 on (u=p) where u<10 and p>2;

select sum(u*p) from tx_n3 join px_n0 on (u=p) where u<10 and p>2;

-- join output estimate is 3 rows ; all the operators stats are "runtime"
explain
select sum(u*p) from tx_n3 join px_n0 on (u=p) where u<10 and p>2;
