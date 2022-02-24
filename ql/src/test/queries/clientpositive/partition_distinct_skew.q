set hive.groupby.skewindata=true;

create table partition_distinct_skew(col1 string, col2 string);
insert into table partition_distinct_skew values('a', 'b'), ('a', 'a'), ('a', 'b');

select col1, col2 from partition_distinct_skew;
explain select col1, count(distinct col2), count(col2) from partition_distinct_skew group by col1;
select col1, count(distinct col2), count(col2)  from partition_distinct_skew group by col1;

explain select col1, count(distinct col2) from partition_distinct_skew group by col1;
select col1, count(distinct col2) from partition_distinct_skew group by col1;

drop table partition_distinct_skew;

