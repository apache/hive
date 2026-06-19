set hive.test.currenttimestamp=2025-04-02 10:05:03;

create table t1 (a int) partitioned by (p_year string);

explain cbo
select * from t1 where p_year IN (
            year(from_unixtime( unix_timestamp() ))
          );
