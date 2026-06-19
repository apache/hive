drop table if exists test_join_1;
drop table if exists test_join_2;

create table test_join_1(a string, b string);
create table test_join_2(a string, b string);

explain
select * from
(
    SELECT a a, b b
    FROM test_join_1
)t1

join

(
    SELECT a a, b b
    FROM test_join_1
)t2
    on  t1.a = t2.a
    and t1.a = t2.b

join

(
    select a from test_join_2
)t3 on t1.a = t3.a;

drop table test_join_1;
drop table test_join_2;
