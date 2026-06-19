CREATE external TABLE table1 (a INT CHECK (a > b) DISABLE, b STRING);
