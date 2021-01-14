--! qt:dataset:src

select * from src
where regexp_extract(get_json_object(src.value, '$.fakekey'), '(h)(.*?)(s)', 2) and src.key > 50;