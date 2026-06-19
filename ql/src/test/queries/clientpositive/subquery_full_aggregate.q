create table alltypestiny(
    smallint_col smallint,
    date_string_col varchar(100),
    string_col varchar(100),
    timestamp_col timestamp,
    bigint_col bigint);

insert into alltypestiny values (1, '2020-03-02', '2020-03-02', '2020-03-02 10:10:10', 1),
(2, '2020-02-02', '2020-02-02', '2020-03-02 10:15:10', 11),
(3, '2020-02-02', '2020-01-02', '2020-03-02 11:10:10', 12),
(null, '2020-03-02', '2020-03-02', '2020-03-02 10:10:10', 1),
(4, '2020-03-02', '2020-03-02', '2020-03-02 10:10:10', null);

explain cbo
SELECT t1.bigint_col
FROM alltypestiny t1
WHERE t1.bigint_col > 1 AND NOT EXISTS
  (SELECT SUM(smallint_col) AS int_col
   FROM alltypestiny
   WHERE t1.date_string_col = string_col AND t1.timestamp_col = timestamp_col)
GROUP BY t1.bigint_col;

SELECT t1.bigint_col
FROM alltypestiny t1
WHERE NOT EXISTS
  (SELECT SUM(smallint_col) AS int_col
   FROM alltypestiny
   WHERE t1.date_string_col = string_col AND t1.timestamp_col = timestamp_col)
GROUP BY t1.bigint_col;


explain cbo
SELECT t1.bigint_col
FROM alltypestiny t1
WHERE EXISTS
  (SELECT SUM(smallint_col) AS int_col
   FROM alltypestiny
   WHERE t1.date_string_col = string_col AND t1.timestamp_col = timestamp_col)
GROUP BY t1.bigint_col;

SELECT t1.bigint_col
FROM alltypestiny t1
WHERE EXISTS
  (SELECT SUM(smallint_col) AS int_col
   FROM alltypestiny
   WHERE t1.date_string_col = string_col AND t1.timestamp_col = timestamp_col)
GROUP BY t1.bigint_col;
