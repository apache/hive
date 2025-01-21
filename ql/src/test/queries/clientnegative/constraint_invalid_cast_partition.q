-- cast('2024-99-99' as date) returns null hence not null constraint violation;

create table t1 (
  date_col date not null
)
partitioned by (p string);

alter table t1 add partition (p = 'a');

insert into t1 partition(p='a') values ('2024-99-99');
