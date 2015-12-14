set hive.mapred.mode=nonstrict;
CREATE TABLE `sample_07` ( `code` string , `description` string , `total_emp` int , `salary` int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile;
set hive.limit.pushdown.memory.usage=0.9999999;

select * from sample_07 order by salary LIMIT 999999999;

SELECT * FROM (
SELECT *, rank() over(PARTITION BY code ORDER BY salary DESC) as rank
FROM sample_07
) ranked_claim
WHERE ranked_claim.rank < 2
ORDER BY code;

select sum(total_emp) over(partition by salary+salary order by code) from sample_07 limit 9999999;