CREATE TABLE showcrt_tab_src (val1 string COMMENT 'val	1', 
                             val2 string COMMENT 'val
                                                2', 
                             val3 string);
INSERT INTO showcrt_tab_src VALUES ('a1	', 'b1', 'c1');

-- Create view from source table with tab in the WHERE clause
CREATE VIEW showcrt_tab_src_v AS SELECT * FROM showcrt_tab_src 
	WHERE val1	= 'a1	'       
	AND val2	= 'b1';
SHOW CREATE TABLE showcrt_tab_src_v;

-- Pattern matching with tabs still need to return correct results
SELECT * FROM showcrt_tab_src_v;

-- Make sure show create table for non-view tables don't break
SHOW CREATE TABLE showcrt_tab_src;

-- Create view with TAB in string literal
CREATE VIEW showcrt_tab_src_v2 AS SELECT 'before
	after' AS col1;
SHOW CREATE TABLE showcrt_tab_src_v2;

CREATE VIEW showcrt_tab_src_v3 AS SELECT '	a		b 	c		' AS col1;
SHOW CREATE TABLE showcrt_tab_src_v3;

CREATE VIEW showcrt_tab_src_v4 AS SELECT 'a\\tb\tc' AS col1;
SHOW CREATE TABLE showcrt_tab_src_v4;

CREATE VIEW showcrt_tab_src_v5 AS SELECT val1 FROM showcrt_tab_src WHERE val1 LIKE 'a%	';
SHOW CREATE TABLE showcrt_tab_src_v5;

CREATE VIEW showcrt_tab_src_v6 AS SELECT "Nested	'string	with	tab' ";
SHOW CREATE TABLE showcrt_tab_src_v6;
SELECT * FROM showcrt_tab_src_v6;
