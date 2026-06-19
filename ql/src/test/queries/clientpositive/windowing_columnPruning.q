--! qt:dataset:part
-- 1. testQueryLevelPartitionColsNotInSelect
select p_size,
sum(p_retailprice) over (distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) as s1
from part 
 ;

-- 2. testWindowPartitionColsNotInSelect
select p_size,
sum(p_retailprice) over (distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) as s1
from part;

-- 3. testHavingColNotInSelect
select p_mfgr,
sum(p_retailprice) over (distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) as s1
from part;
