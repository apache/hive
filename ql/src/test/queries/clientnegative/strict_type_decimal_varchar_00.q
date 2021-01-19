set hive.strict.checks.type.safety=true;
create table chrtbl (varcharcol varchar(5));
select * from chrtbl where varcharcol = 123.2;