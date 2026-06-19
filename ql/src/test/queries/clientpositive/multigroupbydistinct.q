create table tabw4intcols (x integer, y integer, z integer, a integer);
insert into tabw4intcols values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4),
  (1, 2, 1, 2), (2, 3, 2, 3), (3, 4, 3, 4), (4, 1, 4, 1),
  (1, 2, 3, 4), (4, 3, 2, 1), (1, 2, 3, 4), (4, 3, 2, 1);

explain cbo
select z, count(distinct y), count(distinct a)
from tabw4intcols
group by z;

select z, count(distinct y), count(distinct a)
from tabw4intcols
group by z;

explain cbo
select z, x, count(distinct y), count(distinct a)
from tabw4intcols
group by z, x;

select z, x, count(distinct y), count(distinct a)
from tabw4intcols
group by z, x;

explain cbo
select x, z, count(distinct y), count(distinct a)
from tabw4intcols
group by z, x;

select x, z, count(distinct y), count(distinct a)
from tabw4intcols
group by z, x;

explain cbo
select x, a, y, count(distinct z)
from tabw4intcols
group by a, x, y;

select x, a, y, count(distinct z)
from tabw4intcols
group by a, x, y;

explain cbo
select x, count(distinct y), z, count(distinct a)
from tabw4intcols
group by z, x;

select x, count(distinct y), z, count(distinct a)
from tabw4intcols
group by z, x;

explain cbo
select count(distinct y), x, z, count(distinct a)
from tabw4intcols
group by z, x;

select count(distinct y), x, z, count(distinct a)
from tabw4intcols
group by z, x;

drop table tabw4intcols;
