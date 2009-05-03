set hive.cli.errors.ignore=true;
drop table external2;
create external table external2(a int, b int) partitioned by (ds string);
alter table external2 add partition (ds='2008-01-01') location 's3n://data.s3ndemo.hive/pkv/2008-01-01';
describe external2 partition (ds='2008-01-01');
