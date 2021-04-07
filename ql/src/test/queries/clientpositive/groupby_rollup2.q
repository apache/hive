--! qt:dataset:src

create table t (a integer);
insert into t values (1),(2),(null);


select a,count(1),count(a) from t group by a with rollup;
select a,count(a) from t group by a GROUPING SETS ((),(a));


explain
SELECT key, value, count(key) FROM src GROUP BY key, value with rollup;

SELECT key, value, count(key) FROM src GROUP BY key, value with rollup;
