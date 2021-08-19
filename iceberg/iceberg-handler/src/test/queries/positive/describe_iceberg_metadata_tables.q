set hive.vectorized.execution.enabled = false;
set tez.mrreader.config.update.properties=hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids;
set hive.query.results.cache.enabled=false;
set hive.fetch.task.conversion=none;
set hive.cbo.enable=true;

drop table if exists ice_meta_desc;
create external table ice_meta_desc (id int, value string) stored by iceberg stored as orc;
insert into ice_meta_desc values (1, 'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');
insert into ice_meta_desc values (3,'three'),(4,'four'),(5,'five');

describe default.ice_meta_desc;
describe default.ice_meta_desc;
describe default.ice_meta_desc;

describe default.ice_meta_desc.files;
describe default.ice_meta_desc.entries;
describe default.ice_meta_desc.history;
describe default.ice_meta_desc.manifests;
describe default.ice_meta_desc.snapshots;
describe default.ice_meta_desc.partitions;
describe default.ice_meta_desc.all_manifests;
describe default.ice_meta_desc.all_data_files;
describe default.ice_meta_desc.all_entries;

describe formatted default.ice_meta_desc.files;
describe formatted default.ice_meta_desc.entries;
describe formatted default.ice_meta_desc.history;
describe formatted default.ice_meta_desc.manifests;
describe formatted default.ice_meta_desc.snapshots;
describe formatted default.ice_meta_desc.partitions;
describe formatted default.ice_meta_desc.all_manifests;
describe formatted default.ice_meta_desc.all_data_files;
describe formatted default.ice_meta_desc.all_entries;

describe extended default.ice_meta_desc.files;
describe extended default.ice_meta_desc.entries;
describe extended default.ice_meta_desc.history;
describe extended default.ice_meta_desc.manifests;
describe extended default.ice_meta_desc.snapshots;
describe extended default.ice_meta_desc.partitions;
describe extended default.ice_meta_desc.all_manifests;
describe extended default.ice_meta_desc.all_data_files;
describe extended default.ice_meta_desc.all_entries;

drop table ice_meta_desc;
