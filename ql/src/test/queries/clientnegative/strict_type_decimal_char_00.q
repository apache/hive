set hive.strict.checks.type.safety=true;
create table chrtbl (charcol char(5));
select * from chrtbl where charcol = 123.2;