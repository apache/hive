DROP VIEW xxx6;

-- Can't use DROP TABLE on a view
CREATE VIEW xxx6 AS SELECT key FROM src;
DROP TABLE xxx6;
