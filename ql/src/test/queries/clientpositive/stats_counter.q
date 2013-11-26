set hive.stats.autogather=true;
set hive.stats.dbclass=counter;

create table dummy as select * from src;

desc formatted dummy;
