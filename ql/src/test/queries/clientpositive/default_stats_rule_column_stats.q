CREATE TABLE default_stats_rule_test (id int, val string);
INSERT INTO default_stats_rule_test VALUES (1, NULL), (2, NULL), (3, 'x');
ANALYZE TABLE default_stats_rule_test COMPUTE STATISTICS FOR COLUMNS;

-- TopNKey uses DefaultStatsRule; confirm no PARTIAL stats anymore
EXPLAIN SELECT val FROM default_stats_rule_test ORDER BY val LIMIT 1;
