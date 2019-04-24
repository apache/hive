-- HIVE-21531

create temporary table null_tab(x int) stored as orc;
create temporary table dummy_tab(x int) stored as textfile;

-- this disables vectorization for the text file
set hive.vectorized.use.vector.serde.deserialize=false;
set hive.vectorized.use.row.serde.deserialize=false;

insert into null_tab values(1);
insert into dummy_tab values(1);

set hive.explain.user=false;

explain vectorization detail
SELECT MIN(table_name) as table_name, c1,c2,c3 from( 
select 'a' as table_name, null as c1, null as c2, null as c3 from null_tab 
union all 
select 'b' as table_name, null as c1, null as c2, null as c3 from dummy_tab 
) t_union 
group by c1,c2,c3;

SELECT MIN(table_name) as table_name, c1,c2,c3 from( 
select 'a' as table_name, null as c1, null as c2, null as c3 from null_tab 
union all 
select 'b' as table_name, null as c1, null as c2, null as c3 from dummy_tab 
) t_union 
group by c1,c2,c3;

