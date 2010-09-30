set hive.mapjoin.cache.numrows=100;

SELECT  /*+ MAPJOIN(b) */ sum(a.key) as sum_a
	FROM srcpart a
	JOIN src b ON a.key = b.key where a.ds is not null;
