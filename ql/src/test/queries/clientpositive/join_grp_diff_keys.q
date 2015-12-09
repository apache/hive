set hive.mapred.mode=nonstrict;
create table split    (id int, line_id int, orders string);
create table bar      (id int, line_id int, orders string);
create table foo      (id int, line_id int, orders string);
create table forecast (id int, line_id int, orders string);

set hive.auto.convert.join.noconditionaltask=false;

explain 
SELECT foo.id, count(*) as factor from 
 foo JOIN bar  ON (foo.id = bar.id and foo.line_id = bar.line_id) 
 JOIN split    ON (foo.id = split.id and foo.line_id = split.line_id) 
 JOIN forecast ON (foo.id = forecast.id AND foo.line_id = forecast.line_id) 
 WHERE foo.orders != 'blah'  
 group by foo.id;

drop table split;
drop table bar;
drop table foo;
drop table forecast;

reset hive.auto.convert.join.noconditionaltask;
