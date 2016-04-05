create table orcstr (vcol varchar(20)) stored as orc;

insert overwrite table orcstr select null from src;

SET hive.fetch.task.conversion=none;

SET hive.vectorized.execution.enabled=false;
select vcol from orcstr limit 1;

SET hive.vectorized.execution.enabled=true;
select vcol from orcstr limit 1;

insert overwrite table orcstr select "" from src;

SET hive.vectorized.execution.enabled=false;
select vcol from orcstr limit 1;

SET hive.vectorized.execution.enabled=true;
select vcol from orcstr limit 1;

