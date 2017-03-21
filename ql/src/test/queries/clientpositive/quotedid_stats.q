set hive.mapred.mode=nonstrict;

set hive.support.quoted.identifiers=column;

-- escaped back ticks
create table t4(`x+1``` string, `y&y` string);
describe formatted t4;

analyze table t4 compute statistics for columns;

describe formatted t4;
