-- Tests for the most common equality operations between struct types
create table table_struct_types (id int, c1 struct<f1: int,f2: int>, c2 struct<f1: int,f2: int>);
insert into table_struct_types VALUES (1, named_struct("f1",1,"f2",1), named_struct("f1",2,"f2",1));
insert into table_struct_types VALUES (2, named_struct("f1",1,"f2",2), named_struct("f1",2,"f2",2));
insert into table_struct_types VALUES (3, named_struct("f1",1,"f2",3), named_struct("f1",2,"f2",3));
insert into table_struct_types VALUES (4, named_struct("f1",1,"f2",4), named_struct("f1",1,"f2",4));
    
select id from table_struct_types where c1 IN (c1); 
select id from table_struct_types where c1 IN (c2); 
select id from table_struct_types where c1 IN (named_struct("f1",1,"f2",1)); 
select id from table_struct_types where named_struct("f1",1,"f2",1) IN (c1); 
select id from table_struct_types where c1 IN (named_struct("f1",1,"f2",1), named_struct("f1",1,"f2",2), named_struct("f1",1,"f2",3)); 
select id from table_struct_types where c1 IN (c2, named_struct("f1",1,"f2",1), named_struct("f1",1,"f2",2), named_struct("f1",1,"f2",3)); 
select id from table_struct_types where named_struct("f1",1,"f2",1) IN (c1, c2); 

select id from table_struct_types where c1 = c1; 
select id from table_struct_types where c1 = c2;
select id from table_struct_types where c1 = named_struct("f1",1,"f2",1);
select id from table_struct_types where named_struct("f1",1,"f2",1) = c1;
select id from table_struct_types where named_struct("f1",1,"f2",1) = named_struct("f1",1,"f2",1);
select id from table_struct_types where named_struct("f1",1,"f2",1) = named_struct("f1",1,"f2",2);

select id from table_struct_types where c1 <> c1; 
select id from table_struct_types where c1 <> c2; 
select id from table_struct_types where c1 <> named_struct("f1",1,"f2",1);
select id from table_struct_types where named_struct("f1",1,"f2",1) <> c1; 
select id from table_struct_types where named_struct("f1",1,"f2",1) <> named_struct("f1",1,"f2",1); 
select id from table_struct_types where named_struct("f1",1,"f2",1) <> named_struct("f1",1,"f2",2); 

select id from table_struct_types where c1 IS DISTINCT FROM c1;
select id from table_struct_types where c1 IS DISTINCT FROM c2;
select id from table_struct_types where c1 IS DISTINCT FROM named_struct("f1",1,"f2",1); 
select id from table_struct_types where named_struct("f1",1,"f2",1) IS DISTINCT FROM c1; 
select id from table_struct_types where named_struct("f1",1,"f2",1) IS DISTINCT FROM named_struct("f1",1,"f2",1); 
select id from table_struct_types where named_struct("f1",1,"f2",1) IS DISTINCT FROM named_struct("f1",1,"f2",2); 

select id from table_struct_types where c1 IS NOT DISTINCT FROM c1;
select id from table_struct_types where c1 IS NOT DISTINCT FROM c2;
select id from table_struct_types where c1 IS NOT DISTINCT FROM named_struct("f1",1,"f2",1); 
select id from table_struct_types where named_struct("f1",1,"f2",1) IS NOT DISTINCT FROM c1;  
select id from table_struct_types where named_struct("f1",1,"f2",1) IS NOT DISTINCT FROM named_struct("f1",1,"f2",1); 
select id from table_struct_types where named_struct("f1",1,"f2",1) IS NOT DISTINCT FROM named_struct("f1",1,"f2",2);