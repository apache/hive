CREATE DATABASE IF NOT EXISTS test_grouping_set;
CREATE TABLE test_grouping_set.test_grouping(
id string,
s_outlook string,
m_outlook string,
f_outlook string,
s_rating string,
m_rating string,
f_rating string)
CLUSTERED BY (
id)
INTO 2 BUCKETS
;
-- Run query with grouping sets
SELECT
id,
CASE
WHEN grouping(id, s_outlook) = 0 THEN 'outlook'
WHEN grouping(id, m_outlook)= 0 THEN 'outlook'
WHEN grouping(id, f_outlook)= 0 THEN 'outlook'
WHEN grouping(id, s_rating)= 0 THEN 'rating'
WHEN grouping(id, m_rating)= 0 THEN 'rating'
WHEN grouping(id, f_rating)= 0 THEN 'rating'
ELSE NULL
END type,
CASE
WHEN grouping(id, s_outlook) = 0 THEN 'S'
WHEN grouping(id, m_outlook)= 0 THEN 'M'
WHEN grouping(id, f_outlook)= 0 THEN 'F'
WHEN grouping(id, s_rating)= 0 THEN 'S'
WHEN grouping(id, m_rating)= 0 THEN 'M'
WHEN grouping(id, f_rating)= 0 THEN 'F'
ELSE NULL
END agency,
CASE
WHEN grouping(id, s_outlook) = 0 THEN s_outlook
WHEN grouping(id, m_outlook)= 0 THEN m_outlook
WHEN grouping(id, f_outlook)= 0 THEN f_outlook
WHEN grouping(id, s_rating)= 0 THEN s_rating
WHEN grouping(id, m_rating)= 0 THEN m_rating
WHEN grouping(id, f_rating)= 0 THEN f_rating
ELSE NULL
END value
FROM test_grouping_set.test_grouping
GROUP BY
id,
s_outlook,
m_outlook,
f_outlook,
s_rating,
m_rating,
f_rating
GROUPING SETS (
(id, s_outlook),
(id, m_outlook),
(id, f_outlook),
(id, s_rating),
(id, m_rating),
(id, f_rating)
)
;

-- Create view with same query
CREATE OR REPLACE VIEW test_view AS
SELECT
id,
CASE
WHEN grouping(id, s_outlook) = 0 THEN 'outlook'
WHEN grouping(id, m_outlook)= 0 THEN 'outlook'
WHEN grouping(id, f_outlook)= 0 THEN 'outlook'
WHEN grouping(id, s_rating)= 0 THEN 'rating'
WHEN grouping(id, m_rating)= 0 THEN 'rating'
WHEN grouping(id, f_rating)= 0 THEN 'rating'
ELSE NULL
END type,
CASE
WHEN grouping(id, s_outlook) = 0 THEN 'S'
WHEN grouping(id, m_outlook)= 0 THEN 'M'
WHEN grouping(id, f_outlook)= 0 THEN 'F'
WHEN grouping(id, s_rating)= 0 THEN 'S'
WHEN grouping(id, m_rating)= 0 THEN 'M'
WHEN grouping(id, f_rating)= 0 THEN 'F'
ELSE NULL
END agency,
CASE
WHEN grouping(id, s_outlook) = 0 THEN s_outlook
WHEN grouping(id, m_outlook)= 0 THEN m_outlook
WHEN grouping(id, f_outlook)= 0 THEN f_outlook
WHEN grouping(id, s_rating)= 0 THEN s_rating
WHEN grouping(id, m_rating)= 0 THEN m_rating
WHEN grouping(id, f_rating)= 0 THEN f_rating
ELSE NULL
END value
FROM test_grouping_set.test_grouping
GROUP BY
id,
s_outlook,
m_outlook,
f_outlook,
s_rating,
m_rating,
f_rating
GROUPING SETS (
(id, s_outlook),
(id, m_outlook),
(id, f_outlook),
(id, s_rating),
(id, m_rating),
(id, f_rating)
)
;
-- Run query on view including grouping sets
show create table test_view;
select * from test_view;

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
select * from your_view;
