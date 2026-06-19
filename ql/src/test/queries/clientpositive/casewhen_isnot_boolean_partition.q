create table t1(id int) partitioned by (month int, year string);
insert into t1 select 2,2,'2020';
insert into t1 select 3,3,'2024';
insert into t1 select 4,4,'2020';

select id from t1 where (CASE WHEN (month=3) THEN true else false END);
select id from t1 where (CASE WHEN (month=3) THEN false else true END);
select id from t1 where (CASE WHEN (year='2024') THEN true else false END);
select id from t1 where (CASE WHEN (year='2024') THEN false else true END);

select id from t1 where (CASE WHEN (month=3 and year='2024') THEN true else false END);
select id from t1 where (CASE WHEN (month=3 and year='2024') THEN false else true END);

select id from t1 where (CASE WHEN (month=3 or month=4) THEN true else false END);
select id from t1 where (CASE WHEN (month=3 or month=4) THEN false else true END);

select id from t1 where (month=3) is not true;
select id from t1 where (month=3) is true;
select id from t1 where (month=3) is not false;
select id from t1 where (month=3) is false;

select id from t1 where (month=3 or month=4) is not true;
select id from t1 where (month=3 or month=4) is true;
select id from t1 where (month=3 or month=4) is not false;
select id from t1 where (month=3 or month=4) is false;

select id from t1 where (month=3 and id=3) is not true;
select id from t1 where (month=3 and id=3) is true;
select id from t1 where (month=3 and id=3) is not false;
select id from t1 where (month=3 and id=3) is false;

select id from t1 where (month=3) and (id=3) is not true;
select id from t1 where (month=3) and (id=3) is true;
select id from t1 where (month=3) and (id=3) is not false;
select id from t1 where (month=3) and (id=3) is false;

select id from t1 where ((month=3 or month=4) and year='2024') is not true;
select id from t1 where ((month=3 or month=4) and year='2024') is true;
select id from t1 where ((month=3 or month=4) and year='2024') is not false;
select id from t1 where ((month=3 or month=4) and year='2024') is false;

select id from t1 where month in (3,4) is not true;
select id from t1 where month not in (3,4) is not true;
select id from t1 where month in (3,4) is not false;
select id from t1 where month not in (3,4) is not false;
select id from t1 where month in (3,4) is  true;
select id from t1 where month not in (3,4) is true;
select id from t1 where month in (3,4) is  false;
select id from t1 where month not in (3,4) is false;

set hive.cbo.enable=false;
select id from t1 where (CASE WHEN (month=3) THEN true else false END);
select id from t1 where (CASE WHEN (month=3) THEN false else true END);
select id from t1 where (CASE WHEN (year='2024') THEN true else false END);
select id from t1 where (CASE WHEN (year='2024') THEN false else true END);

select id from t1 where (CASE WHEN (month=3 and year='2024') THEN true else false END);
select id from t1 where (CASE WHEN (month=3 and year='2024') THEN false else true END);

select id from t1 where (CASE WHEN (month=3 or month=4) THEN true else false END);
select id from t1 where (CASE WHEN (month=3 or month=4) THEN false else true END);

select id from t1 where (month=3) is not true;
select id from t1 where (month=3) is true;
select id from t1 where (month=3) is not false;
select id from t1 where (month=3) is false;

select id from t1 where (month=3 or month=4) is not true;
select id from t1 where (month=3 or month=4) is true;
select id from t1 where (month=3 or month=4) is not false;
select id from t1 where (month=3 or month=4) is false;

select id from t1 where (month=3 and id=3) is not true;
select id from t1 where (month=3 and id=3) is true;
select id from t1 where (month=3 and id=3) is not false;
select id from t1 where (month=3 and id=3) is false;

select id from t1 where (month=3) and (id=3) is not true;
select id from t1 where (month=3) and (id=3) is true;
select id from t1 where (month=3) and (id=3) is not false;
select id from t1 where (month=3) and (id=3) is false;

select id from t1 where ((month=3 or month=4) and year='2024') is not true;
select id from t1 where ((month=3 or month=4) and year='2024') is true;
select id from t1 where ((month=3 or month=4) and year='2024') is not false;
select id from t1 where ((month=3 or month=4) and year='2024') is false;

select id from t1 where month in (3,4) is not true;
select id from t1 where month not in (3,4) is not true;
select id from t1 where month in (3,4) is not false;
select id from t1 where month not in (3,4) is not false;
select id from t1 where month in (3,4) is  true;
select id from t1 where month not in (3,4) is true;
select id from t1 where month in (3,4) is  false;
select id from t1 where month not in (3,4) is false;