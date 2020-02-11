--! qt:dataset:src
-- year-month/day-time intervals not compatible
select interval_day_time(interval '1' year) from src limit 1;
