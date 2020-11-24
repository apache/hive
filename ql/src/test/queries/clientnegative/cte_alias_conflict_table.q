drop table if exists game_info;
drop table if exists game_info_extend;
create table game_info (game_name string);
create table game_info_extend (ext_id bigint, dev_app_id bigint, game_name string, app_type string);

with game_info as (
select distinct ext_id, dev_app_id, game_name
from game_info_extend )
select count(game_name) from game_info;
