set hive.optimize.point.lookup.min=2;

create table r_table (
  string_col varchar(30)
);

create table l_table (
  string_col varchar(14)
);

insert into r_table VALUES ('AAA111');
insert into l_table VALUES ('AAA111');

explain cbo
SELECT l_table.string_col from l_table, r_table
WHERE r_table.string_col = l_table.string_col AND l_table.string_col IN ('AAA111', 'BBB222') AND r_table.string_col IN ('AAA111', 'BBB222');

SELECT l_table.string_col from l_table, r_table
WHERE r_table.string_col = l_table.string_col AND l_table.string_col IN ('AAA111', 'BBB222') AND r_table.string_col IN ('AAA111', 'BBB222');
