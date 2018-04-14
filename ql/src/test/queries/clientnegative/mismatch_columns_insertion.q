--! qt:dataset:srcpart

create table mismatch_columns(key string, value string);

insert overwrite table mismatch_columns select key from srcpart where ds is not null;
