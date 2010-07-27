
set mapred.reduce.tasks=4
set hive.exec.reducers.max=4
SELECT percentile_approx(cast(substr(src.value,5) AS double), 0.5) FROM src;
SELECT percentile_approx(cast(substr(src.value,5) AS double), 0.5, 100) FROM src;
SELECT percentile_approx(cast(substr(src.value,5) AS double), 0.5, 1000) FROM src;

SELECT percentile_approx(cast(substr(src.value,5) AS int), 0.5) FROM src;
SELECT percentile_approx(cast(substr(src.value,5) AS int), 0.5, 100) FROM src;
SELECT percentile_approx(cast(substr(src.value,5) AS int), 0.5, 1000) FROM src;

SELECT percentile_approx(cast(substr(src.value,5) AS double), array(0.05,0.5,0.95,0.98)) FROM src;
SELECT percentile_approx(cast(substr(src.value,5) AS double), array(0.05,0.5,0.95,0.98), 100) FROM src;
SELECT percentile_approx(cast(substr(src.value,5) AS double), array(0.05,0.5,0.95,0.98), 1000) FROM src;

SELECT percentile_approx(cast(substr(src.value,5) AS int), array(0.05,0.5,0.95,0.98)) FROM src;
SELECT percentile_approx(cast(substr(src.value,5) AS int), array(0.05,0.5,0.95,0.98), 100) FROM src;
SELECT percentile_approx(cast(substr(src.value,5) AS int), array(0.05,0.5,0.95,0.98), 1000) FROM src;
