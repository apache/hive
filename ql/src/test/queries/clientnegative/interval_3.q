--! qt:dataset:src
-- year-month/day-time intervals not compatible
select interval '1' year + interval '365' day from src limit 1;

