DESCRIBE FUNCTION substring_index;
DESCRIBE FUNCTION EXTENDED substring_index;

explain select substring_index('www.apache.org', '.', 2);

select
substring_index('www.apache.org', '.', 3),
substring_index('www.apache.org', '.', 2),
substring_index('www.apache.org', '.', 1),
substring_index('www.apache.org', '.', 0),
substring_index('www.apache.org', '.', -1),
substring_index('www.apache.org', '.', -2),
substring_index('www.apache.org', '.', -3);

select
--str is empty string
substring_index('', '.', 2),
--delim is empty string
substring_index('www.apache.org', '', 1),
--delim does not exist in str
substring_index('www.apache.org', '-', 2),
--delim is two chars
substring_index('www||apache||org', '||', 2),
--null
substring_index(cast(null as string), '.', 2),
substring_index('www.apache.org', cast(null as string), 2),
substring_index('www.apache.org', '.', cast(null as int));

--varchar and char
select
substring_index(cast('www.apache.org' as varchar(20)), '.', 2),
substring_index(cast('www.apache.org' as char(20)), '.', 2);
