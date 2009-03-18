-- scanning partitioned data
explain extended select a.* from srcpart a where rand(1) < 0.1 and a.ds = '2008-04-08';
select a.* from srcpart a where rand(1) < 0.1 and a.ds = '2008-04-08';
