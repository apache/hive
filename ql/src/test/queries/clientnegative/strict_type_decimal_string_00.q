set hive.strict.checks.type.safety=true;
create table strtbl (strcol string);
select * from strtbl where strcol = 123.3;