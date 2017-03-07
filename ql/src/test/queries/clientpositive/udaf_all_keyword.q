-- following tests are to test ALL quantifier (HIVE-16064)

-- count
select count(ALL 1) from src;
select count(ALL key) from src;
-- count(all) and count() should return same value
select count(ALL key) = count(key) from src;

-- AVG
select AVG(ALL 1) from src;
select AVG(ALL key) from src;
select AVG(ALL key) = AVG(key) from src;

-- MIN
select MIN(ALL 1) from src;
select MIN(ALL key) from src;
select MIN(ALL key) = MIN(key) from src;

-- MAX
select MAX(ALL 1) from src;
select MAX(ALL key) from src;
select MAX(ALL key) = MAX(key) from src;

-- SUM
select SUM(ALL 1) from src;
select SUM(ALL key) from src;
select SUM(ALL key) = SUM(key) from src;

