--! qt:dataset:src
SELECT explode(map(1,'one',2,'two',3,'three')) as (myKey,myVal) FROM src GROUP BY key;
