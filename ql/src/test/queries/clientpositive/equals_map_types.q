-- Tests for the most common equality operations between map types
create table table_map_types (id int, c1 map<int,int>, c2 map<int,int>);
insert into table_map_types VALUES (1, map(1,1), map(2,1));
insert into table_map_types VALUES (2, map(1,2), map(2,2));
insert into table_map_types VALUES (3, map(1,3), map(2,3));
insert into table_map_types VALUES (4, map(1,4), map(1,4));
    
select id from table_map_types where c1 IN (c1); 
select id from table_map_types where c1 IN (c2); 
select id from table_map_types where c1 IN (map(1,1)); 
select id from table_map_types where map(1,1) IN (c1); 
select id from table_map_types where c1 IN (map(1,1), map(1,2), map(1,3)); 
select id from table_map_types where c1 IN (c2, map(1,1), map(1,2), map(1,3)); 
select id from table_map_types where map(1,1) IN (c1, c2); 

select id from table_map_types where c1 = c1; 
select id from table_map_types where c1 = c2;
select id from table_map_types where c1 = map(1,1);
select id from table_map_types where map(1,1) = c1;
select id from table_map_types where map(1,1) = map(1,1);
select id from table_map_types where map(1,1) = map(1,2);

select id from table_map_types where c1 <> c1; 
select id from table_map_types where c1 <> c2; 
select id from table_map_types where c1 <> map(1,1);
select id from table_map_types where map(1,1) <> c1; 
select id from table_map_types where map(1,1) <> map(1,1); 
select id from table_map_types where map(1,1) <> map(1,2); 

select id from table_map_types where c1 IS DISTINCT FROM c1;
select id from table_map_types where c1 IS DISTINCT FROM c2;
select id from table_map_types where c1 IS DISTINCT FROM map(1,1); 
select id from table_map_types where map(1,1) IS DISTINCT FROM c1; 
select id from table_map_types where map(1,1) IS DISTINCT FROM map(1,1); 
select id from table_map_types where map(1,1) IS DISTINCT FROM map(1,2); 

select id from table_map_types where c1 IS NOT DISTINCT FROM c1;
select id from table_map_types where c1 IS NOT DISTINCT FROM c2;
select id from table_map_types where c1 IS NOT DISTINCT FROM map(1,1); 
select id from table_map_types where map(1,1) IS NOT DISTINCT FROM c1;  
select id from table_map_types where map(1,1) IS NOT DISTINCT FROM map(1,1); 
select id from table_map_types where map(1,1) IS NOT DISTINCT FROM map(1,2);