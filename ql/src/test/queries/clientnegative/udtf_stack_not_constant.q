--! qt:dataset:alltypesparquet
SELECT STACK(cint, 'a', 'b') FROM alltypesparquet;
