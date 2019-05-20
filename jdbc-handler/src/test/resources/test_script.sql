DROP TABLE IF EXISTS test_strategy;

CREATE TABLE IF NOT EXISTS test_strategy (
  strategy_id int(11) NOT NULL,
  name varchar(50) NOT NULL,
  referrer varchar(1024) DEFAULT NULL,
  landing varchar(1024) DEFAULT NULL,
  priority int(11) DEFAULT NULL,
  implementation varchar(512) DEFAULT NULL,
  last_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (strategy_id)
);


INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (1,'S1','aaa','abc',1000,NULL,'2012-05-08 15:01:15');
INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (2,'S2','bbb','def',990,NULL,'2012-05-08 15:01:15');
INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (3,'S3','ccc','ghi',1000,NULL,'2012-05-08 15:01:15');
INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (4,'S4','ddd','jkl',980,NULL,'2012-05-08 15:01:15');
INSERT INTO test_strategy (strategy_id, name, referrer, landing, priority, implementation, last_modified) VALUES (5,'S5','eee',NULL,NULL,NULL,'2012-05-08 15:01:15');


