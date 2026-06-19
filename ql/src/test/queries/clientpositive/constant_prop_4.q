create table cx2(bool1 boolean);
insert into cx2 values (true),(false),(null);

set hive.cbo.enable=true;
select bool1 IS TRUE OR (cast(NULL as boolean) AND bool1 IS NOT TRUE AND bool1 IS NOT FALSE) from cx2;

set hive.cbo.enable=false;
select bool1 IS TRUE OR (cast(NULL as boolean) AND bool1 IS NOT TRUE AND bool1 IS NOT FALSE) from cx2;

