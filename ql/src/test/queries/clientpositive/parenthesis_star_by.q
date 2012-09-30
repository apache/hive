SELECT key, value FROM src CLUSTER BY key, value;
SELECT key, value FROM src ORDER BY key ASC, value ASC;
SELECT key, value FROM src SORT BY key, value;
SELECT key, value FROM src DISTRIBUTE BY key, value;


SELECT key, value FROM src CLUSTER BY (key, value);
SELECT key, value FROM src ORDER BY (key ASC, value ASC);
SELECT key, value FROM src SORT BY (key, value);
SELECT key, value FROM src DISTRIBUTE BY (key, value);
