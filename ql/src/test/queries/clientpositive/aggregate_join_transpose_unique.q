-- SORT_QUERY_RESULTS
-- Aggregate arguments that come from the unique side of the join must survive
-- the unique-based aggregate-join transpose (hive.transpose.aggr.join.unique)
create table ajt_dim (x int, k int, y int, primary key (k) disable novalidate rely);
create table ajt_dim_nopk (k int, x int);
create table ajt_fact (fk int);
insert into ajt_dim values (10, 1, 100), (20, 2, 200), (null, 3, 300);
insert into ajt_dim_nopk values (1, 10), (2, 20), (3, null);
insert into ajt_fact values (1), (1), (2), (3), (3), (3);

explain cbo
select k, sum(x), min(x), max(x), count(x), count(*) from ajt_dim join ajt_fact on k = fk group by k;
select k, sum(x), min(x), max(x), count(x), count(*) from ajt_dim join ajt_fact on k = fk group by k;

explain cbo
select k, y, sum(x), sum(y) from ajt_dim join ajt_fact on k = fk group by k, y;
select k, y, sum(x), sum(y) from ajt_dim join ajt_fact on k = fk group by k, y;

explain cbo
select d.k, sum(d.x) from (select k, max(x) x from ajt_dim_nopk group by k) d join ajt_fact on d.k = fk group by d.k;
select d.k, sum(d.x) from (select k, max(x) x from ajt_dim_nopk group by k) d join ajt_fact on d.k = fk group by d.k;

set hive.transpose.aggr.join.unique=false;
select k, sum(x), min(x), max(x), count(x), count(*) from ajt_dim join ajt_fact on k = fk group by k;
select k, y, sum(x), sum(y) from ajt_dim join ajt_fact on k = fk group by k, y;
select d.k, sum(d.x) from (select k, max(x) x from ajt_dim_nopk group by k) d join ajt_fact on d.k = fk group by d.k;
