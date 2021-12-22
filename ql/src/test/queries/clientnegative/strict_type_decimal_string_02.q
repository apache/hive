set hive.strict.checks.type.safety=true;
create table tbl (strcol string, deccol decimal(4,1));
select * from tbl where deccol = strcol;