DESCRIBE FUNCTION stddev_samp;
DESCRIBE FUNCTION EXTENDED stddev_samp;
DESCRIBE FUNCTION stddev_samp;
DESCRIBE FUNCTION EXTENDED stddev_samp;


drop table if exists t_n23;
create table t_n23 (a int);
insert into t_n23 values (1),(-1),(0);

select stddev_samp(a) from t_n23;
select stddev_samp(a) from t_n23 where a=0;
select round(stddev_samp(a),5) from t_n23 where a>=0;
