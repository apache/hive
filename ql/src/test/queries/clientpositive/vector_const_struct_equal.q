set hive.fetch.task.conversion=none;
set hive.cbo.enable=false;

create table test (a string);
insert into test values ('aa');

--expect all rows
select * from test where (struct(2022) = struct(2022));
select * from test where (struct('2022') = struct('2022'));
select * from test where (struct(2022.0D) = struct(2022.0D));

--expect empty result
select * from test where (struct(2022) = struct(2023));
select * from test where (struct('2022') = struct('2023'));
select * from test where (struct(2022.0D) = struct(2023.0D));
