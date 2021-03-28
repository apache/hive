set hive.query.results.cache.enabled=false;
set hive.optimize.bi.enabled=true;

create table sketch_input (id int, category char(1));

-- count distinct -> hll
explain cbo physical
select category, count(distinct id) from sketch_input group by category;

-- rank -> kll
explain cbo physical
select id, rank() over (order by id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from sketch_input order by id;

explain cbo physical
select id, count(id) over ()*rank() over (order by id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from sketch_input order by id;

-- rank_partition_by -> kll
explain cbo physical
select id, rank() over (partition by category order by id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from sketch_input order by category,id;

-- ntile -> kll
explain cbo physical
select id, ntile(4) over (order by id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from sketch_input order by id;

-- ntile_partition_by -> kll
explain cbo physical
select id, ntile(4) over (partition by category order by id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from sketch_input order by category,id;

-- cume_dist -> kll
explain cbo physical
select id, cume_dist() over (order by id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from sketch_input order by id;

explain cbo physical
select id, count(id) over ()*cume_dist() over (order by id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from sketch_input order by id;

-- cume_dist_partition_by -> kll
explain cbo physical
select id, cume_dist() over (partition by category order by id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from sketch_input order by category,id;

drop table sketch_input;
