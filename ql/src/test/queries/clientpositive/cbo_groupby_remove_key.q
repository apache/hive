CREATE TABLE passenger
(
    id       INT    NOT NULL,
    fname    STRING NOT NULL,
    lname    STRING NOT NULL,
    passport STRING NOT NULL,
    UNIQUE (id) DISABLE RELY,
    UNIQUE (passport) DISABLE RELY,
    UNIQUE (fname, lname) DISABLE RELY
);

EXPLAIN CBO SELECT id, COUNT(1) FROM passenger GROUP BY id, passport;
EXPLAIN CBO SELECT passport, COUNT(1) FROM passenger GROUP BY id, passport;
EXPLAIN CBO SELECT id, COUNT(1) FROM passenger GROUP BY id, fname, lname, passport;
EXPLAIN CBO SELECT passport, COUNT(1) FROM passenger GROUP BY id, fname, lname, passport;
EXPLAIN CBO SELECT fname, COUNT(1) FROM passenger GROUP BY id, fname, lname, passport;
EXPLAIN CBO SELECT lname, COUNT(1) FROM passenger GROUP BY id, fname, lname, passport;
EXPLAIN CBO SELECT fname, lname, COUNT(1) FROM passenger GROUP BY id, fname, lname, passport;
