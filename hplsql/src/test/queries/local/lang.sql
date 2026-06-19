-- Integer literals
+1;
1;
0;
-1;

-- Decimal literals
1.0;
+1.0;
-1.0;
-- 1.;
-- +1.;
-- -1.;
-- .1;
-- +.1;
-- -.1;

-- Identifiers
declare abc int;
declare abc_abc int;
declare "abc" int;
declare [abc] int;
declare `abc` int;
declare :new_abc int;
declare @abc int;
declare _abc int;
declare #abc int;
declare ##abc int;
declare $abc int;
declare abc_9 int;

-- Operators and expressions
+1 + 1;                 -- 2
1 + 1;                  -- 2
1 + -1;                 -- 0
-- 'a' + 'b';              -- ab    
-- 'a''b' + 'c';           -- ab''c   
-- 'a\'b' + 'c';           -- ab\'c   
-- 1 + '1'                 -- 2        
-- '1' + 1                 -- 2
-- 1 + 'a'                 -- 1a     
-- 'a' + 1                 -- a1

-1 - 1;   -- -2
-1 - -1;  -- 0

 