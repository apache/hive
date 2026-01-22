create table ice_t (id int, name string, balance int) stored by iceberg TBLPROPERTIES ('format-version'='3');
insert into ice_t values (1, 'aaa', 25),(2, 'bbb', 35),(3, 'ccc', 82),(4, 'ddd', 91);

select id, name, balance, ROW_ID, LAST_UPDATED_SEQUENCE_NUMBER from ice_t order by id;

update ice_t set balance = 500 where id = 2;

select id, name, balance, ROW_ID, LAST_UPDATED_SEQUENCE_NUMBER from ice_t order by id;