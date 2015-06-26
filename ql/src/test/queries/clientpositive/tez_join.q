set hive.explain.user=false;
set hive.auto.convert.sortmerge.join = true;

create table t1(
id string,
od string);

create table t2(
id string,
od string);

explain
select vt1.id from
(select rt1.id from
(select t1.id, t1.od from t1 order by t1.id, t1.od) rt1) vt1
join
(select rt2.id from
(select t2.id, t2.od from t2 order by t2.id, t2.od) rt2) vt2
where vt1.id=vt2.id;

select vt1.id from
(select rt1.id from
(select t1.id, t1.od from t1 order by t1.id, t1.od) rt1) vt1
join
(select rt2.id from
(select t2.id, t2.od from t2 order by t2.id, t2.od) rt2) vt2
where vt1.id=vt2.id;
