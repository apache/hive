-- Tests for the most common equality operations between array types
create table table_list_types (id int, c1 array<int>, c2 array<int>);
insert into table_list_types VALUES (1, array(1,1), array(2,1));
insert into table_list_types VALUES (2, array(1,2), array(2,2));
insert into table_list_types VALUES (3, array(1,3), array(2,3));
insert into table_list_types VALUES (4, array(1,4), array(1,4));
    
select id from table_list_types where c1 IN (c1); 
select id from table_list_types where c1 IN (c2); 
select id from table_list_types where c1 IN (array(1,1)); 
select id from table_list_types where array(1,1) IN (c1); 
select id from table_list_types where c1 IN (array(1,1), array(1,2), array(1,3)); 
select id from table_list_types where c1 IN (c2, array(1,1), array(1,2), array(1,3)); 
select id from table_list_types where array(1,1) IN (c1, c2); 

select id from table_list_types where c1 = c1; 
select id from table_list_types where c1 = c2;
select id from table_list_types where c1 = array(1,1);
select id from table_list_types where array(1,1) = c1;
select id from table_list_types where array(1,1) = array(1,1);
select id from table_list_types where array(1,1) = array(1,2);

select id from table_list_types where c1 <> c1; 
select id from table_list_types where c1 <> c2; 
select id from table_list_types where c1 <> array(1,1);
select id from table_list_types where array(1,1) <> c1; 
select id from table_list_types where array(1,1) <> array(1,1); 
select id from table_list_types where array(1,1) <> array(1,2); 

select id from table_list_types where c1 IS DISTINCT FROM c1;
select id from table_list_types where c1 IS DISTINCT FROM c2;
select id from table_list_types where c1 IS DISTINCT FROM array(1,1); 
select id from table_list_types where array(1,1) IS DISTINCT FROM c1; 
select id from table_list_types where array(1,1) IS DISTINCT FROM array(1,1); 
select id from table_list_types where array(1,1) IS DISTINCT FROM array(1,2); 

select id from table_list_types where c1 IS NOT DISTINCT FROM c1;
select id from table_list_types where c1 IS NOT DISTINCT FROM c2;
select id from table_list_types where c1 IS NOT DISTINCT FROM array(1,1); 
select id from table_list_types where array(1,1) IS NOT DISTINCT FROM c1;  
select id from table_list_types where array(1,1) IS NOT DISTINCT FROM array(1,1); 
select id from table_list_types where array(1,1) IS NOT DISTINCT FROM array(1,2);