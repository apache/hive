SET hive.vectorized.execution.enabled=true;
SET hive.fetch.task.conversion=none;

-- test single repeating key
create temporary table foo (id int, x map<string,int>) stored as orc;
insert into foo values(1, map('ABC', 9)), (2, map('ABC', 7)), (3, map('ABC', 8)), (4, map('ABC', 9));
select id from foo where x['ABC']=9;

-- test multiple repeating keys
create temporary table bar (id int, x map<string,int>) stored as orc;
insert into bar values(1, map('A', 9, 'B', 1)), (2, map('A', 7, 'B', 2)), (3, map('A', 8, 'B', 3)), (4, map('A', 9, 'B', 4));
select id from bar where x['A']=9;

-- test mixed keys
create temporary table doo (id int, x map<string,int>) stored as orc;
insert into doo values(1, map('ABC', 9, 'B', 1)), (2, map('AB', 7)), (3, map('A', 8, 'C', 3)), (4, map('D', 7, 'ABC', 9, 'E', 4));
select id from doo where x['ABC']=9;