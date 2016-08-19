set hive.optimize.skewjoin=true;
set hive.auto.convert.join=false;

drop table if exists decimal_skewjoin_1;
drop table if exists decimal_skewjoin_2;

create table decimal_skewjoin_1 (t decimal(4,2), u decimal(5), v decimal);
create table decimal_skewjoin_2 (t decimal(4,2), u decimal(5), v decimal);

insert overwrite table decimal_skewjoin_1
  select cast('17.29' as decimal(4,2)), 3.1415926BD, 3115926.54321BD from src tablesample (1 rows);

insert overwrite table decimal_skewjoin_2
  select cast('17.29' as decimal(4,2)), 3.1415926BD, 3115926.54321BD from src tablesample (1 rows);

select a.u from decimal_skewjoin_1 a INNER JOIN decimal_skewjoin_2 b ON a.t=b.t;

drop table decimal_skewjoin_1;
drop table decimal_skewjoin_2;
