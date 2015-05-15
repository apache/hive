with src1 as (select key from src order by key limit 5)
select * from src1;

use default;
drop view v;
create view v as with cte as (select key, value from src order by key limit 5)
select key from cte;

describe extended v;

create database bug;
use bug;
select * from default.v;
drop database bug;

use default;
drop view v;
create view v as with cte as (select * from src  order by key limit 5)
select * from cte;

describe extended v;

create database bug;
use bug;
select * from default.v;
drop database bug;


use default;
drop view v;
create view v as with src1 as (select key from src order by key limit 5)
select * from src1;

describe extended v;

create database bug;
use bug;
select * from default.v;
use default;
drop view v;
drop database bug;
