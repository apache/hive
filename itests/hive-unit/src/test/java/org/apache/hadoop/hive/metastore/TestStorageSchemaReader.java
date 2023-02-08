package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hive.storage.jdbc.JdbcSerDe;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestStorageSchemaReader {

  protected HiveConf hiveConf;
  protected HiveMetaStoreClient client;
  protected String dbName;
  protected Map<String, String> avroTableParams = new HashMap<>();
  Map<String, String> hbaseTableParams = new HashMap<>();
  Map<String, String> hbaseSerdeParams = new HashMap<>();
  Map<String, String> jdbcTableParams = new HashMap<>();
  Map<String, String> jdbcSerdeParams = new HashMap<>();

  @Before @BeforeEach public void setUp() throws Exception {
    dbName = "sampleDb";
    hiveConf = new HiveConf(this.getClass());
    new DatabaseBuilder().setName(dbName).create(new HiveMetaStoreClient(hiveConf), hiveConf);
    avroTableParams.put("avro.schema.literal",
        "{\"name\":\"nullable\", \"type\":\"record\", \"fields\":[{\"name\":\"id\", \"type\":\"int\"}, {\"name\":\"value\", \"type\":\"int\"}]}");

    hbaseTableParams.put("storage_handler", "org.apache.hadoop.hive.hbase.HBaseStorageHandler");
    hbaseTableParams.put("hbase.table.name", "t_hive");
    hbaseTableParams.put("hbase.table.default.storage.type", "binary");
    hbaseTableParams.put("external.table.purge", "true");

    hbaseSerdeParams.put("hbase.zookeeper.quorum", "test_host");
    hbaseSerdeParams.put("hbase.zookeeper.property.clientPort", "8765");
    hbaseSerdeParams.put("hbase.table.name", "my#tbl");
    hbaseSerdeParams.put("hbase.columns.mapping", "cf:string");

    jdbcTableParams.put("hive.sql.database.type", "METASTORE");
    jdbcTableParams.put("hive.sql.query", "SELECT \"SERDE_ID\", \"NAME\", \"SLIB\" FROM \"SERDES\"");

    jdbcSerdeParams.put("serialization.format", "1");
    jdbcTableParams.put("storage_handler", "org.apache.hive.storage.jdbc.JdbcStorageHandler");
  }

  @After @AfterEach public void tearDown() throws Exception {
    new HiveMetaStoreClient(hiveConf).dropDatabase(dbName, true, true, true);
  }

  private Table createTable(String tblName, String serdeClass, String inputFormatClass, String outputFormatClass,
      Map<String, String> tableParams, Map<String, String> serdeParams) throws TException {
    client = new HiveMetaStoreClient(hiveConf);
    return new TableBuilder().setDbName(dbName).setTableName(tblName).addCol("id", "int", "comment for " + tblName)
        .addCol("value", "int", "comment for " + tblName).setSerdeLib(serdeClass).setInputFormat(inputFormatClass)
        .setOutputFormat(outputFormatClass).setTableParams(tableParams)
        .addStorageDescriptorParam("test_param_1", "Use this for comments etc").setSerdeParams(serdeParams)
        .create(client, hiveConf);
  }

  private void checkSchema(String tblName, Table tbl) throws TException {
    List<FieldSchema> fieldSchemasFull = client.getSchema(dbName, tblName);
    assertNotNull(fieldSchemasFull);
    assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size());
    checkFields(tbl.getSd().getCols(), fieldSchemasFull);
  }

  private void checkFields(List<FieldSchema> fieldSchemas, List<FieldSchema> fieldSchemasFromHMS) {
    for (int i = 0; i < fieldSchemas.size(); i++) {
      assertTrue(
          fieldSchemas.get(i).getName().equals(fieldSchemasFromHMS.get(i).getName()) && fieldSchemas.get(i).getType()
              .equals(fieldSchemasFromHMS.get(i).getType()));
    }
  }

  @Test public void testAvroTableWithDefaultSSR() throws Exception {
    hiveConf.set("metastore.storage.schema.reader.impl", "org.apache.hadoop.hive.metastore.DefaultStorageSchemaReader");
    String tblName = "avroTable";
    createTable(tblName, AvroSerDe.class.getName(), AvroContainerInputFormat.class.getName(),
        AvroContainerOutputFormat.class.getName(), avroTableParams, new HashMap<>());
    assertThrows("Storage schema reading not supported", MetaException.class, () -> client.getSchema(dbName, tblName));
  }

  @Test public void testAvroTableWithSerdeSSR() throws Exception {
    hiveConf.set("metastore.storage.schema.reader.impl", "org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader");
    String tblName = "avroTable";
    Table tbl = createTable(tblName, AvroSerDe.class.getName(), AvroContainerInputFormat.class.getName(),
        AvroContainerOutputFormat.class.getName(), avroTableParams, new HashMap<>());
    checkSchema(tblName, tbl);
  }

  @Test public void testHbaseTableWithDefaultSSR() throws Exception {
    hiveConf.set("metastore.storage.schema.reader.impl", "org.apache.hadoop.hive.metastore.DefaultStorageSchemaReader");
    String tblName = "jdbcTable";

    createTable(tblName, HBaseSerDe.class.getName(), null, null, hbaseTableParams, hbaseSerdeParams);
    assertThrows("Storage schema reading not supported", MetaException.class, () -> client.getSchema(dbName, tblName));
  }

  @Test public void testHbaseTableWithSerdeSSR() throws Exception {
    hiveConf.set("metastore.storage.schema.reader.impl", "org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader");
    String tblName = "jdbcTable";

    Table table =
        createTable(tblName, "org.apache.hadoop.hive.hbase.HBaseSerDe", null, null, hbaseTableParams, hbaseSerdeParams);
    checkSchema(tblName, table);
  }

  @Test public void testJdbcTableWithDefaultSSR() throws Exception {
    hiveConf.set("metastore.storage.schema.reader.impl", "org.apache.hadoop.hive.metastore.DefaultStorageSchemaReader");
    String tblName = "jdbcTable";

    createTable(tblName, JdbcSerDe.class.getName(), null, null, jdbcTableParams, jdbcSerdeParams);
    assertThrows("Storage schema reading not supported", MetaException.class, () -> client.getSchema(dbName, tblName));
  }

  @Test public void testJdbcTableWithSerdeSSR() throws Exception {
    hiveConf.set("metastore.storage.schema.reader.impl", "org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader");
    String tblName = "jdbcTable";

    Table table = createTable(tblName, JdbcSerDe.class.getName(), null, null, jdbcTableParams, jdbcSerdeParams);
    checkSchema(tblName, table);
  }

  @Test public void testOrcTableWithDefaultSSR() throws Exception {
    hiveConf.set("metastore.storage.schema.reader.impl", "org.apache.hadoop.hive.metastore.DefaultStorageSchemaReader");
    String tblName = "orcTable2";
    Table tbl =
        createTable(tblName, OrcSerde.class.getName(), OrcInputFormat.class.getName(), OrcOutputFormat.class.getName(),
              new HashMap<>(), new HashMap<>());
    checkSchema(tblName, tbl);
  }

  @Test public void testOrcTableWithSerdeSSR() throws Exception {
    hiveConf.set("metastore.storage.schema.reader.impl", "org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader");
    String tblName = "orcTable";
    Table tbl =
        createTable(tblName, OrcSerde.class.getName(), OrcInputFormat.class.getName(), OrcOutputFormat.class.getName(),
            new HashMap<>(), new HashMap<>());
    checkSchema(tblName, tbl);
  }
}
