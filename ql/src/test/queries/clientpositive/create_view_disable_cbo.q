set hive.cbo.enable = false;

CREATE DATABASE IF NOT EXISTS cdh_82023_repro_db;

CREATE TABLE IF NOT EXISTS `cdh_82023_repro_db`.`data` (
  `text` string
);

CREATE VIEW IF NOT EXISTS `cdh_82023_repro_db`.`background` AS
SELECT
  *
FROM
  `cdh_82023_repro_db`.`data` `xouter`
WHERE
  `xouter`.`text` NOT IN (
    SELECT
      UPPER(`xinner`.`text`)
    FROM
      `cdh_82023_repro_db`.`data` `xinner`
    GROUP BY
      UPPER(`xinner`.`text`)
  );

SELECT * FROM `cdh_82023_repro_db`.`background`;

CREATE VIEW IF NOT EXISTS `cdh_82023_repro_db`.`foreground`AS
SELECT
  *
FROM
  `cdh_82023_repro_db`.`background`;

SELECT * FROM `cdh_82023_repro_db`.`foreground`;

DROP DATABASE IF EXISTS `cdh_82023_repro_db` CASCADE;
