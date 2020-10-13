--! qt:dataset:part
-- SORT_QUERY_RESULTS

--empty table
create table tempty(i int, j int);

CREATE TABLE part_null_n0 as select * from part;
insert into part_null_n0 values(NULL,NULL,NULL,NULL,NULL, NULL, NULL,NULL,NULL);

CREATE TABLE part_null_n1 as select * from part;
insert into part_null_n1 values(17273,NULL,NULL,NULL,NULL, NULL, NULL,NULL,NULL);

-- test all six comparison operators
explain cbo select count(*) from part where p_partkey = ANY (select p_partkey from part);
select count(*) from part where p_partkey = ANY (select p_partkey from part);

--explain cbo select count(*) from part where p_partkey <> ANY (select p_partkey from part);
--select count(*) from part where p_partkey <> ANY (select p_partkey from part);

explain cbo select count(*) from part where p_partkey > ANY (select p_partkey from part);
select count(*) from part where p_partkey > ANY (select p_partkey from part);

explain cbo select count(*) from part where p_partkey < ANY (select p_partkey from part);
select count(*) from part where p_partkey < ANY (select p_partkey from part);

explain cbo select count(*) from part where p_partkey >= ANY (select p_partkey from part);
select count(*) from part where p_partkey >= ANY (select p_partkey from part);

explain cbo select count(*) from part where p_partkey <= ANY (select p_partkey from part);
select count(*) from part where p_partkey <= ANY (select p_partkey from part);

-- SOME is same as ANY
explain cbo select count(*) from part where p_partkey = SOME(select min(p_partkey) from part);
select count(*) from part where p_partkey = SOME(select min(p_partkey) from part);

-- ANY with aggregate in subquery
explain cbo select count(*) from part where p_size < ANY (select max(p_size) from part group by p_partkey);
select count(*) from part where p_size < ANY (select max(p_size) from part group by p_partkey);

select count(*) from part where p_size < ANY (select max(null) from part group by p_partkey);

--empty row produces false with ANY
select count(*) from part where p_partkey = ANY(select i from tempty);

-- true + null, should produce results 
select count(*) from part where p_partkey = ANY (select p_partkey from part_null_n0);

-- false + null -> null
select count(*) from part where (p_size= ANY (select p_partkey from part_null_n0)) is null;

-- all null -> null
select count(*) from part where (p_partkey = ANY (select p_partkey from part_null_n0 where p_partkey is null)) is null;

-- false, should produce zero result
select count(*) from part where p_partkey > ANY (select max(p_partkey) from part_null_n0);

-- ANY in having
explain cbo select count(*) from part having count(*) > ANY (select count(*) from part group by p_partkey);
select count(*) from part having count(*) > ANY (select count(*) from part group by p_partkey);

-- multiple
explain cbo select count(*) from part where p_partkey >= ANY (select p_partkey from part) 
	AND p_size = ANY (select p_size from part group by p_size);
select count(*) from part where p_partkey >= ANY (select p_partkey from part) 
	AND p_size = ANY (select p_size from part group by p_size);

--nested
explain cbo select count(*) from part where p_partkey 
	>= ANY (select p_partkey from part where p_size >= ANY(select p_size from part_null_n0 group by p_size)) ;
select count(*) from part where p_partkey 
	>= ANY (select p_partkey from part where p_size >= ANY(select p_size from part_null_n0 group by p_size)) ;

-- subquery in SELECT
select p_partkey, (p_partkey > ANY (select p_partkey from part)) from part;

select p_partkey, (p_partkey > ANY (select p_partkey from part_null_n0)) from part_null_n0;

select p_partkey, (p_partkey > ANY (select null from part_null_n0)) from part_null_n0;

select p_partkey, (p_partkey > ANY (select i from tempty)) from part_null_n0;

-- correlated
explain select * from part where p_partkey > ANY (select p_partkey from part p where p.p_type = part.p_type);
select * from part where p_partkey > ANY (select p_partkey from part p where p.p_type = part.p_type);

-- correlated, select, with empty results, should produce false
explain select p_partkey, (p_partkey >= ANY (select p_partkey from part pp where pp.p_type = part.p_name)) from part;
select p_partkey, (p_partkey >= ANY (select p_partkey from part pp where pp.p_type = part.p_name)) from part;

-- correlated, correlation condtion matches but subquery will not produce result due to false prediate, should produce false
explain select p_partkey, (p_partkey >= ANY (select p_partkey from part pp where pp.p_type = part.p_type and p_partkey < 0)) from part;
select p_partkey, (p_partkey >= ANY (select p_partkey from part pp where pp.p_type = part.p_type and p_partkey < 0)) from part;

-- correlated, subquery has match, should produce true
explain select p_partkey, (p_partkey >= ANY (select p_partkey from part pp where pp.p_type = part.p_type)) from part;
select p_partkey, (p_partkey >= ANY (select p_partkey from part pp where pp.p_type = part.p_type)) from part;

-- correlated, subquery has match but has NULL for one row, should produce one NULL
explain select p_partkey, (p_size >= ANY (select 3*p_size from part_null_n1 pp where pp.p_partkey = part.p_partkey)) from part;
select p_partkey, (p_size >= ANY (select 3*p_size from part_null_n1 pp where pp.p_partkey = part.p_partkey)) from part;

-- correlated, with an aggregate and explicit group by
explain select p_partkey, (p_partkey >= ANY (select min(p_partkey) from part pp where pp.p_type = part.p_name group by p_partkey)) from part;
select p_partkey, (p_partkey >= ANY (select min(p_partkey) from part pp where pp.p_type = part.p_name group by p_partkey)) from part;

-- nested
explain select * from part_null_n1 where p_name IN (select p_name from part where part.p_type = part_null_n1.p_type
                    AND p_size >= ANY(select p_size from part pp where part.p_type = pp.p_type));
select * from part_null_n1 where p_name IN (select p_name from part where part.p_type = part_null_n1.p_type
                    AND p_size >= ANY(select p_size from part pp where part.p_type = pp.p_type));

-- multi
explain select * from part_null_n1 where p_name IN (select p_name from part where part.p_type = part_null_n1.p_type)
                    AND p_size >= ANY(select p_size from part pp where part_null_n1.p_type = pp.p_type);

select * from part_null_n1 where p_name IN (select p_name from part where part.p_type = part_null_n1.p_type)
                    AND p_size >= ANY(select p_size from part pp where part_null_n1.p_type = pp.p_type);

DROP TABLE part_null_n1;
DROP TABLE part_null_n0;
DROP TABLE tempty;
