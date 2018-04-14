--! qt:dataset:src

select
  year(iym), month(iym), day(idt), hour(idt), minute(idt), second(idt)
from (
  select interval '1-2' year to month iym, interval '3 4:5:6.789' day to second idt
  from src limit 1
) q;

