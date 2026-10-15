-- A top-level UDTF's positions refer to its output, not its array argument.
create table paui_src (a int);
insert into paui_src values (1),(3),(2);
create table paui_pos (a int);
create table paui_named (a int);
insert into paui_pos (a)
select explode(array(a)) from paui_src tablesample (5 rows) order by 1 desc limit 1;
insert into paui_named (a)
select explode(array(a)) as c from paui_src tablesample (5 rows) order by c desc limit 1;
select 'position', a from paui_pos;
select 'named', a from paui_named;
