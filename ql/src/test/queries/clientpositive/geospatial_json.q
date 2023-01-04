CREATE TABLE cities (Name string, CoOrdinate binary)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.udf.esri.serde.EsriJsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.esriJson.UnenclosedEsriJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

load data local inpath "../../data/files/geo-json.json" OVERWRITE INTO TABLE cities;

select name, ST_AsText(coordinate) from cities;
