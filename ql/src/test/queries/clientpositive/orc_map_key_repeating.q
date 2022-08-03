SET hive.vectorized.execution.enabled=true;
SET hive.fetch.task.conversion=none;

create temporary table foo (id int, x map<string,int>) stored as orc;
insert into foo values(1, map('ABC', 9)), (2, map('ABC', 7)), (3, map('ABC', 8)), (4, map('ABC', 9));
select id from foo where x['ABC']=9;