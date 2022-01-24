set hive.optimize.join.disjunctive.transitive.predicates.pushdown=false;

CREATE TABLE test1 (act_nbr string);
CREATE TABLE test2 (month int);
CREATE TABLE test3 (mth int, con_usd double);

EXPLAIN CBO
SELECT c.month,
      d.con_usd
FROM
 (SELECT cast(regexp_replace(substr(add_months(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), -1), 1, 7), '-', '') AS int) AS month
  FROM test1
  UNION ALL
  SELECT month
  FROM test2
  WHERE month = 202110) c
JOIN test3 d ON c.month = d.mth;
