set hive.cbo.enable=false;
create table ctas_dup_col_noncbo as select 'a' as c, 'b' as c;
