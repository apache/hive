create table repro (lvalue int, charstring string) stored as parquet;

LOAD DATA LOCAL INPATH '../../data/files/escape_crlf.parquet' overwrite into table repro;

set hive.fetch.task.conversion=more;


select count(*) from repro;

set hive.cli.print.escape.crlf=false;
select * from repro;


set hive.cli.print.escape.crlf=true;
select * from repro;

