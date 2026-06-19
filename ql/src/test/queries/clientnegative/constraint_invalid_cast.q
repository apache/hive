-- cast('2024-99-99' as date) returns null hence not null constraint violation;

create table t1(
  date_col date not null
);

insert into t1 values ('2024-99-99');
