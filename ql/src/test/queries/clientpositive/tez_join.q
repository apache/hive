set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.sortmerge.join = true;

create table t1_n42(
id string,
od string);

create table t2_n26(
id string,
od string);

explain
select vt1_n42.id from
(select rt1_n42.id from
(select t1_n42.id, t1_n42.od from t1_n42 order by t1_n42.id, t1_n42.od) rt1_n42) vt1_n42
join
(select rt2_n26.id from
(select t2_n26.id, t2_n26.od from t2_n26 order by t2_n26.id, t2_n26.od) rt2_n26) vt2_n26
where vt1_n42.id=vt2_n26.id;

select vt1_n42.id from
(select rt1_n42.id from
(select t1_n42.id, t1_n42.od from t1_n42 order by t1_n42.id, t1_n42.od) rt1_n42) vt1_n42
join
(select rt2_n26.id from
(select t2_n26.id, t2_n26.od from t2_n26 order by t2_n26.id, t2_n26.od) rt2_n26) vt2_n26
where vt1_n42.id=vt2_n26.id;
