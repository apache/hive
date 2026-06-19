explain
select timestamp '2011-01-01 01:01:01';
select timestamp '2011-01-01 01:01:01';

explain
select '2011-01-01 01:01:01.101' <> timestamp '2011-01-01 01:01:01.100';
select '2011-01-01 01:01:01.101' <> timestamp '2011-01-01 01:01:01.100';

explain
select 1 where timestamp '2011-01-01 01:01:01.101' <> timestamp '2011-01-01 01:01:01.100';
select 1 where timestamp '2011-01-01 01:01:01.101' <> timestamp '2011-01-01 01:01:01.100';

