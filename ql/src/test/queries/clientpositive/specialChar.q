set hive.mapred.mode=nonstrict;

-- 'hel'lo'
-- 'hel;';lo'
-- 'h;el'lo'
-- 'hel"lo'

CREATE TABLE ts(s varchar(550));

INSERT INTO ts VALUES ('Mozilla/5.0 (iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3');

INSERT INTO ts VALUES ("Mozilla/5.0 (iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3");

INSERT INTO ts VALUES ("Mozilla/5.0 (iPhone\; \\; \\\;CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3");

INSERT INTO ts VALUES ("Mozilla/5.0 (iPhone\/; \/\/\/;CPU \;\;\;iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3");

INSERT INTO ts VALUES ('\'');

INSERT INTO ts VALUES ('\"');

INSERT INTO ts VALUES ("Mozilla\"\'/5.0 \"(iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3");

INSERT INTO ts VALUES ("Mozilla\'\"/5.0 \'(iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3");

INSERT INTO ts VALUES ("Mozilla\\\"/5.0 ;;;;;;(iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3");

INSERT INTO ts VALUES ("Mozilla\'\\/5.0 ;;;\";;\";(iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3");

select * from ts order by s;
