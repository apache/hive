SELECT pmod(null, null)
FROM src LIMIT 1;

SELECT pmod(-100,9), pmod(-50,101), pmod(-1000,29)
FROM src LIMIT 1;

SELECT pmod(100,19), pmod(50,125), pmod(300,15)
FROM src LIMIT 1;
