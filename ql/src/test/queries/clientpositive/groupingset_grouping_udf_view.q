CREATE TABLE your_table(col1 int, col2 int, col3 int);
create view your_view as SELECT
    col1,
    col2,
    SUM(col3),
    GROUPING(col1) AS is_col1_grouped
FROM
    your_table
GROUP BY
    col1, col2
WITH CUBE;

show create table your_view;
describe formatted your_view;
select * from your_view;
