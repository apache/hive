--! qt:dataset:src

set hive.stats.column.autogather=false;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
set hive.fetch.task.conversion=none;
set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.groupby.skewindata=false;


create table t (a integer);
insert into t values (1),(2),(null);

select grouping(a),a from t group by a with rollup;

select grouping(a),a,count(a) from t group by a with rollup;

select a,count(1),count(a) from t group by a with rollup;
select a,count(a) from t group by a GROUPING SETS ((),(a));



explain
select grouping(a),count(distinct a) from t group by a with rollup;
select grouping(a),count(distinct a) from t group by a with rollup;

explain
SELECT grouping(key) gk, grouping(value) gv, key, value, count(key) FROM src where key<10 GROUP BY key, value with rollup order by gk,gv,key;

SELECT grouping(key) gk, grouping(value) gv, key, value, count(key) FROM src where key<10 GROUP BY key, value with rollup order by gk,gv,key;
