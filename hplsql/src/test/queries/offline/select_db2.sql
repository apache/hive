select coalesce(max(info_id)+1,0) into NextID from sproc_info with rr use and keep exclusive locks;

select cd, cd + inc days, cd - inc days + coalesce(inc, 0) days
from (select date '2015-09-02' as cd, 3 as inc from sysibm.sysdummy1);

