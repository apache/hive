--! qt:dataset:src1
--! qt:dataset:src
with src1 as (select key from src order by key limit 5)
select * from src1;

use default;
drop view v_n0;
create view v_n0 as with cte as (select key, value from src order by key limit 5)
select key from cte;

describe extended v_n0;

create database bug;
use bug;
select * from default.v_n0;
drop database bug;

use default;
drop view v_n0;
create view v_n0 as with cte as (select * from src  order by key limit 5)
select * from cte;

describe extended v_n0;

create database bug;
use bug;
select * from default.v_n0;
drop database bug;


use default;
drop view v_n0;
create view v_n0 as with src1 as (select key from src order by key limit 5)
select * from src1;

describe extended v_n0;

create database bug;
use bug;
select * from default.v_n0;
use default;
drop view v_n0;

create view v_n0 as with src1 as (select key from src order by key limit 5)
select * from src1 a where a.key is not null;

describe extended v_n0;
select * from v_n0;
drop view v_n0;

drop database bug;
