DESCRIBE FUNCTION stddev_samp;
DESCRIBE FUNCTION EXTENDED stddev_samp;
DESCRIBE FUNCTION stddev_samp;
DESCRIBE FUNCTION EXTENDED stddev_samp;


drop table if exists t;
create table t (a int);
insert into t values (1),(-1),(0);

select stddev_samp(a) from t;
select stddev_samp(a) from t where a=0;
select round(stddev_samp(a),5) from t where a>=0;
