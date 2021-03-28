set hive.query.results.cache.enabled=false;

create table sketch_input (id int, category char(1));

-- get unique count estimates per category
explain cbo physical
select category, ds_hll_estimate(ds_hll_sketch(id)) from sketch_input group by category;

explain
select category, ds_hll_estimate(ds_hll_sketch(id)) from sketch_input group by category;

-- union sketches across categories and get overall unique count estimate
explain cbo physical
select ds_hll_estimate(ds_hll_union(sketch)) from
(
  select ds_hll_sketch(id) as sketch from sketch_input group by category
) sketch_intermediate;

explain
select ds_hll_estimate(ds_hll_union(sketch)) from
(
  select ds_hll_sketch(id) as sketch from sketch_input group by category
) sketch_intermediate;

-- rank
explain cbo physical
select id,
		rank() over (order by id),
		case when ds_kll_n(ds) < (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1) then ds_kll_n(ds) else (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1) end
from sketch_input
join ( select ds_kll_sketch(cast(id as float)) as ds from sketch_input ) q
order by id;

explain
select id,
		rank() over (order by id),
		case when ds_kll_n(ds) < (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1) then ds_kll_n(ds) else (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1) end
from sketch_input
join ( select ds_kll_sketch(cast(id as float)) as ds from sketch_input ) q
order by id;

-- rank_partition_by
explain cbo physical
select id,category,
		rank() over (partition by category order by id),
		case when ds_kll_n(ds) < (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1) then ds_kll_n(ds) else (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1) end
from sketch_input
join ( select category as c,ds_kll_sketch(cast(id as float)) as ds from sketch_input group by category) q on (q.c=category)
order by category,id;

explain
select id,category,
		rank() over (partition by category order by id),
		case when ds_kll_n(ds) < (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1) then ds_kll_n(ds) else (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1) end
from sketch_input
join ( select category as c,ds_kll_sketch(cast(id as float)) as ds from sketch_input group by category) q on (q.c=category)
order by category,id;

-- ntile
explain cbo physical
select id,
	ntile(4) over (order by id),
	case when ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*4) < 1 then 1 else ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*4) end
from sketch_input
join ( select ds_kll_sketch(cast(id as float)) as ds from sketch_input ) q
order by id;

explain
select id,
	ntile(4) over (order by id),
	case when ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*4) < 1 then 1 else ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*4) end
from sketch_input
join ( select ds_kll_sketch(cast(id as float)) as ds from sketch_input ) q
order by id;

-- ntile_partition_by
explain cbo physical
select id,category,
		ntile(4) over (partition by category order by id),
		case when ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*4) < 1 then 1 else ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*4) end
from sketch_input
join ( select category as c,ds_kll_sketch(cast(id as float)) as ds from sketch_input group by category) q on (q.c=category)
order by category,id;

explain
select id,category,
		ntile(4) over (partition by category order by id),
		case when ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*4) < 1 then 1 else ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*4) end
from sketch_input
join ( select category as c,ds_kll_sketch(cast(id as float)) as ds from sketch_input group by category) q on (q.c=category)
order by category,id;

-- cume_dist
explain cbo physical
select id,category,cume_dist() over (partition by category order by id),ds_kll_rank(ds, CAST(id AS FLOAT))
from sketch_input
join ( select category as c,ds_kll_sketch(cast(id as float)) as ds from sketch_input group by category) q on (q.c=category)
order by category,id;

explain
select id,category,cume_dist() over (partition by category order by id),ds_kll_rank(ds, CAST(id AS FLOAT))
from sketch_input
join ( select category as c,ds_kll_sketch(cast(id as float)) as ds from sketch_input group by category) q on (q.c=category)
order by category,id;

drop table sketch_input;
