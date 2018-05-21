DESCRIBE FUNCTION var_samp;
DESCRIBE FUNCTION EXTENDED var_samp;
DESCRIBE FUNCTION var_samp;
DESCRIBE FUNCTION EXTENDED var_samp;

drop table if exists t_n27;
create table t_n27 (a int);
insert into t_n27 values (1),(-1),(0);

select var_samp(a) from t_n27;
select var_samp(a) from t_n27 where a=0;
select round(var_samp(a),5) from t_n27 where a>=0;

