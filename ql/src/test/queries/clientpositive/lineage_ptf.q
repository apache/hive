set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;

-- Create a source table for testing
create table source_tbl2(col_001 int, col_002 int, col_003 int, p1 int);

create view b_v_5_x as
select *
from (select col_001, row_number() over (order by src.p1) as r_num
        from source_tbl2 src) v1;

-- Test baseline 1: single select with 2 columns
select col_002, p1 from source_tbl2;

-- Test baseline 2: single select with a single column
select col_002 from source_tbl2;

-- Test baseline 3: single select with a function call
select max(p1) from source_tbl2;

-- Test baseline 4: single select with a constant, a column and a function call
select 1, col_001, PI() from source_tbl2;

--- Views for single PTF operators

-- Test for ROW_NUMBER
create view b_v_4_0 as
select *
from (
    select col_001,
        row_number() over (partition by src.p1) as r_num,
        row_number() over (partition by src.col_002) as r_num2,
        row_number() over (partition by src.col_002) as r_num3
    from source_tbl2 src) v1;

-- Test for ROW_NUMBER
create view b_v_4 as
select *
from (select col_001, row_number() over (partition by src.p1) as r_num
        from source_tbl2 src) v1;

create view b_v_5 as
select *
from (select col_001, row_number() over (order by src.p1) as r_num
        from source_tbl2 src) v1;

-- Test for RANK
create view b_v_6 as
select *
from (select col_001, rank() over (partition by src.p1) as r_num
        from source_tbl2 src) v1;

-- Test for DENSE_RANK
create view b_v_8 as
select *
from (select col_001, dense_rank() over (partition by src.p1) as r_num
        from source_tbl2 src) v1;
        
-- Test for NTILE (dividing data into 4 groups)
create view b_v_9 as
select *
from (select col_001, ntile(4) over (order by src.col_002) as r_num
        from source_tbl2 src) v1;

-- Test for PERCENT_RANK
create view b_v_10 as
select *
from (select col_001, percent_rank() over (partition by src.p1 order by src.col_002) as r_num
        from source_tbl2 src) v1;
        
-- Test for LAG (previous row's value)
create view b_v_11 as
select *
from (select col_001, lag(src.col_002, 1, 0) over (partition by src.p1 order by src.col_001) as r_num
        from source_tbl2 src) v1;

-- Test for LEAD (next row's value)
create view b_v_12 as
select *
from (select col_001, lead(src.col_002, 1, 0) over (partition by src.p1 order by src.col_001) as r_num
        from source_tbl2 src) v1;

-- Test for FIRST_VALUE
create view b_v_13 as
select *
from (select col_001, first_value(src.col_002) over (partition by src.p1 order by src.col_001) as r_num
        from source_tbl2 src) v1;

-- Test for LAST_VALUE
create view b_v_14 as
select *
from (select col_001, last_value(src.col_002) over (partition by src.p1 order by src.col_001) as r_num
        from source_tbl2 src) v1;

-- Test for CUME_DIST
create view b_v_15 as
select *
from (select col_001, cume_dist() over (partition by src.p1 order by src.col_002) as r_num
        from source_tbl2 src) v1;

-- Test for PERCENTILE_CONT
create view b_v_16 as
select *
from (select percentile_cont(0.5) within group (order by src.col_002) as r_num
        from source_tbl2 src) v1;

-- Test for PERCENTILE_DISC
create view b_v_17 as
select *
from (select percentile_disc(0.5) within group (order by src.col_002) as r_num
        from source_tbl2 src) v1;

--- Views for complex use cases

-- Test for calculating a running sum of col_002 within each partition
create view b_v_18 as
select *
from (select col_001, sum(src.col_002) over (partition by src.p1 order by src.col_001) as r_num
        from source_tbl2 src) v1;

-- Test for comparing a row's value to the previous row's value
create view b_v_19 as
select *
from (select col_001, col_002 - lag(src.col_002, 1, 0) over (partition by src.p1 order by src.col_001) as value_diff
        from source_tbl2 src) v1;

-- Test for finding the average of the current and two previous rows (rolling average)
create view b_v_20 as
select *
from (select col_001, avg(src.col_002) over (partition by src.p1 order by src.col_001 rows between 2 preceding and current row) as r_num
        from source_tbl2 src) v1;

-- Test for partition by two columns
create view b_v_21 as
select *
from (select col_001, row_number() over (partition by src.p1, src.col_003 order by src.col_001) as r_num
        from source_tbl2 src) v1;

-- Test for order by two columns
create view b_v_22 as
select *
from (select col_001, row_number() over (partition by src.p1 order by src.col_002, src.col_001) as r_num
        from source_tbl2 src) v1;

-- Test for reuse the same column multiple times
create view b_v_23 as
select *
from (select col_001, sum(src.col_001) over (partition by src.col_001 order by src.col_001, src.col_001) as r_num
        from source_tbl2 src) v1;
