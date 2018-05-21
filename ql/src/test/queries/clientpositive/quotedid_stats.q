set hive.mapred.mode=nonstrict;

set hive.support.quoted.identifiers=column;

-- escaped back ticks
create table t4_n9(`x+1``` string, `y&y` string);
describe formatted t4_n9;

analyze table t4_n9 compute statistics for columns;

describe formatted t4_n9;
