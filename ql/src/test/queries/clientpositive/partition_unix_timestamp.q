-- HiveFilter(condition=[=(CAST($1):DOUBLE, 2.0250E3)])
--! qt:replace:/(.*,\s)[0-9.E]+(\)\])/$1#Masked#$2/

create table t1 (a int) partitioned by (p_year string);

explain cbo
select * from t1 where p_year IN (
            year(from_unixtime( unix_timestamp() ))
          );
