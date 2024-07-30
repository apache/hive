set hive.strict.checks.type.safety=true;
create table dectbl (deccol decimal(4,1));
select * from dectbl where deccol = '123.3';