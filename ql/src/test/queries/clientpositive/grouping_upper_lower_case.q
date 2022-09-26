
create table test(
    col1 string,
    col2 int
);

select grouping(col2) from test group by col2 with rollup;
select GROUPING(col2) from test group by col2 with rollup;
