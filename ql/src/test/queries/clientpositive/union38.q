-- SORT_QUERY_RESULTS
--create table union38_test1 

create table union38_test1( name String, id int, address String); 
insert into union38_test1 values("Young", 1, "Sydney"), ("Jin", 2, "Mel"); 
analyze table union38_test1 compute statistics for columns; 

create view union38_test1_view as select * from union38_test1; 
select * from union38_test1_view; 


--create table union38_test2 

create table union38_test2( name String, id int, address String); 
insert into union38_test2 values("Eun", 3, "Bri"), ("Kim", 4, "Ad"); 

create view union38_test2_view as select * from union38_test2; 
select * from union38_test2_view; 

select * from union38_test1 union select * from union38_test2; 
select * from union38_test1_view union select * from union38_test2_view; 


--create view union38_test_view using tables with union 

create view union38_test_view as select * from union38_test1 union select * from union38_test2; 
select * from union38_test_view; 


--create view union38_test_view using tables with union all 

create view union38_test_view1 as select * from union38_test1 union all select * from union38_test2; 
select * from union38_test_view1; 


--create view union38_test_view using temp table with union 

create view union38_test_view2 as with union38_tmp_1 as ( select * from union38_test1 ), union38_tmp_2 as (select * from union38_test2 ) select * from union38_tmp_1 union select * from union38_tmp_2; 
select * from union38_test_view2; 


--create view union38_test_view using temp table with union all 

create view union38_test_view3 as with union38_tmp_1 as ( select * from union38_test1 ), union38_tmp_2 as (select * from union38_test2 ) select * from union38_tmp_1 union all select * from union38_tmp_2; 
select * from union38_test_view3; 


--create table test_table using temp table with union all 

create table union38_test_table1 as with union38_tmp_1 as ( select * from union38_test1 ), union38_tmp_2 as (select * from union38_test2 ) select * from union38_tmp_1 union all select * from union38_tmp_2; 
select * from union38_test_table1; 
