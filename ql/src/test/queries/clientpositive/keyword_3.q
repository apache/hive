drop table NULLS;

create table NULLS (LAST string);

insert overwrite table NULLS
  select key from src where key = '238' limit 1;

select LAST from NULLS;
