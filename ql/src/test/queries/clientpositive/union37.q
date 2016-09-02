create table l_test1 (id bigint,val string,trans_date string) row format delimited fields terminated by ' ' ;
insert into l_test1 values (1, "table_1", "2016-08-11");

create table l_test2 (id bigint,val string,trans_date string) row format delimited fields terminated by ' ' ;  
insert into l_test2 values (2, "table_2", "2016-08-11");

explain
select 
    id,
    'table_1' ,
    trans_date
from l_test1
union all
select 
    id,
    val,
    trans_date
from l_test2 ;

select 
    id,
    'table_1' ,
    trans_date
from l_test1
union all
select 
    id,
    val,
    trans_date
from l_test2 ;

explain
select 
    id,
    999,
    'table_1' ,
    trans_date
from l_test1
union all
select 
    id,
    999,
    val,
    trans_date
from l_test2 ;

select 
    id,
    999,
    'table_1' ,
    trans_date
from l_test1
union all
select 
    id,
    999,
    val,
    trans_date
from l_test2 ;

explain
select 
    id,
    999,
    666,
    'table_1' ,
    trans_date
from l_test1
union all
select 
    id,
    999,
    666,
    val,
    trans_date
from l_test2 ;

select 
    id,
    999,
    666,
    'table_1' ,
    trans_date
from l_test1
union all
select 
    id,
    999,
    666,
    val,
    trans_date
from l_test2 ;

explain
select 
    id,
    999,
    'table_1' ,
    trans_date,
    '2016-11-11'
from l_test1
union all
select 
    id,
    999,
    val,
    trans_date,
    trans_date
from l_test2 ;

select 
    id,
    999,
    'table_1' ,
    trans_date,
    '2016-11-11'
from l_test1
union all
select 
    id,
    999,
    val,
    trans_date,
    trans_date
from l_test2 ;
