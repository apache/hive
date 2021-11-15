set hive.strict.checks.type.safety=true;
create table tbl (varchrcol varchar(5), deccol decimal(4,1));
select * from tbl where deccol < varchrcol;