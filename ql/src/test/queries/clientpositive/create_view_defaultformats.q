--! qt:dataset:src
drop view if exists sfsrc;
drop view if exists rcsrc;
set hive.default.fileformat=SequenceFile;
create view sfsrc as select * from src;
set hive.default.fileformat=RcFile;
create view rcsrc as select * from src;
describe formatted sfsrc;
describe formatted rcsrc;
select * from sfsrc where key = 100 limit 1;
select * from rcsrc where key = 100 limit 1;
drop view sfsrc;
drop view rcsrc;
set hive.default.fileformat=TextFile;

