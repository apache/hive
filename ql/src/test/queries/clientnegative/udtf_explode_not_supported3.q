--! qt:dataset:src
select explode(array(1),array(2)) as myCol from src;