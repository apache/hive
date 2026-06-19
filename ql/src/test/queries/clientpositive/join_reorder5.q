-- Project shouldn't contain unnecessary fields in CBO plan.
-- Instead of
--   HiveProject(wr_order_number=[$0], wr_returned_time_sk=[$1], wr_return_quantity=[$2], BLOCK__OFFSET__INSIDE__FILE=[$3], INPUT__FILE__NAME=[$4], ROW__ID=[$5])
-- Project should look like
--   HiveProject(wr_order_number=[$0])

create table web_sales (
    ws_order_number int
);

create table web_returns (
    wr_order_number int,
    wr_returned_time_sk timestamp,
    wr_return_quantity int
);

explain cbo
with ws_wh as
    (select ws1.ws_order_number
    from web_sales ws1,web_returns wr2
    where ws1.ws_order_number = wr2.wr_order_number)
select
   ws_order_number
from
   web_sales ws1
where
ws1.ws_order_number in (select wr_order_number
                            from web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number);
