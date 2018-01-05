package org.apache.hadoop.hive.metastore.schema.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.DefaultStorageSchemaReader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.AvroSchemaUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestDefaultStorageSchemaReader {
  private static final Logger LOG = LoggerFactory.getLogger(TestDefaultStorageSchemaReader.class);
  private static final String TEST_DB_NAME = "TEST_DB";
  private static final String TEST_TABLE_NAME = "TEST_TABLE";
  private HiveMetaStoreClient client;
  private Configuration conf;
  private Warehouse warehouse;
  private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;
  private static final String AVRO_SERIALIZATION_LIB =
      "org.apache.hadoop.hive.serde2.avro.AvroSerDe";

  // These schemata are used in other tests
  static public final String MAP_WITH_PRIMITIVE_VALUE_TYPE = "{\n" +
      "  \"namespace\": \"testing\",\n" +
      "  \"name\": \"oneMap\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aMap\",\n" +
      "      \"type\":{\"type\":\"map\",\n" +
      "      \"values\":\"long\"}\n" +
      "\t}\n" +
      "  ]\n" +
      "}";
  static public final String ARRAY_WITH_PRIMITIVE_ELEMENT_TYPE = "{\n" +
      "  \"namespace\": \"testing\",\n" +
      "  \"name\": \"oneArray\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"anArray\",\n" +
      "      \"type\":{\"type\":\"array\",\n" +
      "      \"items\":\"string\"}\n" +
      "\t}\n" +
      "  ]\n" +
      "}";
  public static final String RECORD_SCHEMA = "{\n" +
      "  \"namespace\": \"testing.test.mctesty\",\n" +
      "  \"name\": \"oneRecord\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aRecord\",\n" +
      "      \"type\":{\"type\":\"record\",\n" +
      "              \"name\":\"recordWithinARecord\",\n" +
      "              \"fields\": [\n" +
      "                 {\n" +
      "                  \"name\":\"int1\",\n" +
      "                  \"type\":\"int\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"boolean1\",\n" +
      "                  \"type\":\"boolean\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"name\":\"long1\",\n" +
      "                  \"type\":\"long\"\n" +
      "                }\n" +
      "      ]}\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String NULLABLE_RECORD_SCHEMA = "[\"null\", " + RECORD_SCHEMA + "]";
  public static final String UNION_SCHEMA = "{\n" +
      "  \"namespace\": \"test.a.rossa\",\n" +
      "  \"name\": \"oneUnion\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aUnion\",\n" +
      "      \"type\":[\"int\", \"string\"]\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String UNION_SCHEMA_2 = "{\n" +
      "  \"namespace\": \"test.a.rossa\",\n" +
      "  \"name\": \"oneUnion\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aUnion\",\n" +
      "      \"type\":[\"null\", \"int\", \"string\"]\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String UNION_SCHEMA_3 = "{\n" +
      "  \"namespace\": \"test.a.rossa\",\n" +
      "  \"name\": \"oneUnion\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aUnion\",\n" +
      "      \"type\":[\"float\",\"int\"]\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String UNION_SCHEMA_4 = "{\n" +
      "  \"namespace\": \"test.a.rossa\",\n" +
      "  \"name\": \"oneUnion\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aUnion\",\n" +
      "      \"type\":[\"int\",\"float\",\"long\"]\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String ENUM_SCHEMA = "{\n" +
      "  \"namespace\": \"clever.namespace.name.in.space\",\n" +
      "  \"name\": \"oneEnum\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "   {\n" +
      "      \"name\":\"baddies\",\n" +
      "      \"type\":{\"type\":\"enum\",\"name\":\"villians\", \"symbols\": " +
      "[\"DALEKS\", \"CYBERMEN\", \"SLITHEEN\", \"JAGRAFESS\"]}\n" +
      "      \n" +
      "      \n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String FIXED_SCHEMA = "{\n" +
      "  \"namespace\": \"ecapseman\",\n" +
      "  \"name\": \"oneFixed\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "   {\n" +
      "      \"name\":\"hash\",\n" +
      "      \"type\":{\"type\": \"fixed\", \"name\": \"MD5\", \"size\": 16}\n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String NULLABLE_STRING_SCHEMA = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"nullableUnionTest\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"nullableString\", \"type\":[\"null\", \"string\"]}\n" +
      "  ]\n" +
      "}";
  public static final String MAP_WITH_NULLABLE_PRIMITIVE_VALUE_TYPE_SCHEMA = "{\n" +
      "  \"namespace\": \"testing\",\n" +
      "  \"name\": \"mapWithNullableUnionTest\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"aMap\",\n" +
      "      \"type\":{\"type\":\"map\",\n" +
      "      \"values\":[\"null\",\"long\"]}\n" +
      "\t}\n" +
      "  ]\n" +
      "}";
  public static final String NULLABLE_ENUM_SCHEMA = "{\n" +
      "  \"namespace\": \"clever.namespace.name.in.space\",\n" +
      "  \"name\": \"nullableUnionTest\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "   {\n" +
      "      \"name\":\"nullableEnum\",\n" +
      "      \"type\": [\"null\", {\"type\":\"enum\",\"name\":\"villians\", \"symbols\": " +
      "[\"DALEKS\", \"CYBERMEN\", \"SLITHEEN\", \"JAGRAFESS\"]}]\n" +
      "      \n" +
      "      \n" +
      "    }\n" +
      "  ]\n" +
      "}";
  public static final String BYTES_SCHEMA = "{\n" +
      "  \"type\": \"record\", \n" +
      "  \"name\": \"bytesTest\",\n" +
      "  \"fields\" : [\n" +
      "    {\"name\":\"bytesField\", \"type\":\"bytes\"}\n" +
      "  ]\n" +
      "}";

  public static final String KITCHEN_SINK_SCHEMA = "{\n" +
      "  \"namespace\": \"org.apache.hadoop.hive\",\n" +
      "  \"name\": \"kitchsink\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\":\"string1\",\n" +
      "      \"type\":\"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"string2\",\n" +
      "      \"type\":\"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"int1\",\n" +
      "      \"type\":\"int\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"boolean1\",\n" +
      "      \"type\":\"boolean\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"long1\",\n" +
      "      \"type\":\"long\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"float1\",\n" +
      "      \"type\":\"float\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"double1\",\n" +
      "      \"type\":\"double\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"inner_record1\",\n" +
      "      \"type\":{ \"type\":\"record\",\n" +
      "               \"name\":\"inner_record1_impl\",\n" +
      "               \"fields\": [\n" +
      "                          {\"name\":\"int_in_inner_record1\",\n" +
      "                           \"type\":\"int\"},\n" +
      "                          {\"name\":\"string_in_inner_record1\",\n" +
      "                           \"type\":\"string\"}\n" +
      "                         ]\n" +
      "       }\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"enum1\",\n" +
      "      \"type\":{\"type\":\"enum\", \"name\":\"enum1_values\", " +
      "\"symbols\":[\"ENUM1_VALUES_VALUE1\",\"ENUM1_VALUES_VALUE2\", \"ENUM1_VALUES_VALUE3\"]}\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"array1\",\n" +
      "      \"type\":{\"type\":\"array\", \"items\":\"string\"}\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"map1\",\n" +
      "      \"type\":{\"type\":\"map\", \"values\":\"string\"}\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"union1\",\n" +
      "      \"type\":[\"float\", \"boolean\", \"string\"]\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"fixed1\",\n" +
      "      \"type\":{\"type\":\"fixed\", \"name\":\"fourbytes\", \"size\":4}\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"null1\",\n" +
      "      \"type\":\"null\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"UnionNullInt\",\n" +
      "      \"type\":[\"int\", \"null\"]\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\":\"bytes1\",\n" +
      "      \"type\":\"bytes\"\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    warehouse = new Warehouse(conf);

    // set some values to use for getting conf. vars
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    conf.set("hive.key1", "value1");
    conf.set("hive.key2", "http://www.example.com");
    conf.set("hive.key3", "");
    conf.set("hive.key4", "0");
    conf.set("datanucleus.autoCreateTables", "false");

    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX, 2);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.LIMIT_PARTITION_REQUEST,
        DEFAULT_LIMIT_PARTITION_REQUEST);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.STORAGE_SCHEMA_READER_IMPL,
        DefaultStorageSchemaReader.class.getName());
    client = createClient();
  }

  @After
  public void closeClient() {
    client.close();
  }

  private void silentDropDatabase(String dbName) throws TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException | InvalidOperationException e) {
      // NOP
    }
  }

  private HiveMetaStoreClient createClient() throws Exception {
    try {
      return new HiveMetaStoreClient(conf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  @Test
  public void testSimpleAvroTable() throws TException, IOException {
    List<FieldSchema> fields = new ArrayList<>(2);
    FieldSchema field = new FieldSchema();
    field.setName("name");
    field.setType("string");
    field.setComment("Test name comment");
    fields.add(field);

    field = new FieldSchema();
    field.setName("age");
    field.setType("int");
    field.setComment("Test age comment");
    fields.add(field);

    createTable(TEST_DB_NAME, TEST_TABLE_NAME, AVRO_SERIALIZATION_LIB, fields, null);
    List<FieldSchema> retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    verifyTableFields(fields, retFields, null);
  }

  private Table createTable(String dbName, String tblName, String serializationLib,
      List<FieldSchema> fields, Map<String, String> tblProperties) throws TException, IOException {
    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
    db = client.getDatabase(dbName);
    Path dbPath = new Path(db.getLocationUri());
    FileSystem fs = FileSystem.get(dbPath.toUri(), conf);
    String typeName = "dummy";
    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(fields);
    client.createType(typ1);

    Table tbl = new TableBuilder().setDbName(dbName).setTableName(tblName).setCols(typ1.getFields())
        .setSerdeLib(serializationLib).setTableParams(tblProperties).build();
    client.createTable(tbl);
    return client.getTable(dbName, tblName);
  }

  @Test
  public void testExternalSchemaAvroTable() throws TException, IOException {
    //map
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, MAP_WITH_PRIMITIVE_VALUE_TYPE);
    List<FieldSchema> retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "aMap", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "map<string,bigint>",
        retFields.get(0).getType());
    Assert.assertEquals("Unexpected comment of the field", "", retFields.get(0).getComment());

    //list
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME,
        ARRAY_WITH_PRIMITIVE_ELEMENT_TYPE);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "anArray", retFields.get(0).getName());
    Assert
        .assertEquals("Unexpected type of the field", "array<string>", retFields.get(0).getType());

    //struct
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, RECORD_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "aRecord", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field",
        "struct<int1:int,boolean1:boolean,long1:bigint>", retFields.get(0).getType());

    //union
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, UNION_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "aUnion", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "uniontype<int,string>",
        retFields.get(0).getType());

    //union-2
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, UNION_SCHEMA_2);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "aUnion", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "uniontype<int,string>",
        retFields.get(0).getType());

    //union_3
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, UNION_SCHEMA_3);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "aUnion", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "uniontype<float,int>",
        retFields.get(0).getType());

    //union_4
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, UNION_SCHEMA_4);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "aUnion", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "uniontype<int,float,bigint>",
        retFields.get(0).getType());

    //enum
    // Enums are one of two Avro types that Hive doesn't have any native support for.
    // Column names - we lose the enumness of this schema
    // Column types become string
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, ENUM_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "baddies", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "string",
        retFields.get(0).getType());

    // Hive has no concept of Avro's fixed type.  Fixed -> arrays of bytes
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, FIXED_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "hash", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "binary",
        retFields.get(0).getType());

    //nullable string
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, NULLABLE_STRING_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "nullableString", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "string",
        retFields.get(0).getType());

    //map with nullable value - That Union[T, NULL] is converted to just T, within a Map
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, MAP_WITH_NULLABLE_PRIMITIVE_VALUE_TYPE_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "aMap", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "map<string,bigint>",
        retFields.get(0).getType());

    // That Union[T, NULL] is converted to just T.
    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, NULLABLE_ENUM_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "nullableEnum", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "string",
        retFields.get(0).getType());

    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, BYTES_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 1, retFields.size());
    Assert.assertEquals("Unexpected name of the field", "bytesField", retFields.get(0).getName());
    Assert.assertEquals("Unexpected type of the field", "binary",
        retFields.get(0).getType());

    createAvroTableWithExternalSchema(TEST_DB_NAME, TEST_TABLE_NAME, KITCHEN_SINK_SCHEMA);
    retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    Assert.assertEquals("Unexpected number of fields", 16, retFields.size());
    //There are 16 fields in this schema. Instead of verifying all we verify the interesting ones
    //(ones which have not been tested above)
    Assert
        .assertEquals("Unexpected name of 8th field", "inner_record1", retFields.get(7).getName());
    Assert.assertEquals("Unexpected type of the field",
        "struct<int_in_inner_record1:int,string_in_inner_record1:string>",
        retFields.get(7).getType());

    Assert.assertEquals("Unexpected field name of the 10th field", "array1",
        retFields.get(9).getName());
    Assert.assertEquals("Unexpected field type of the 10th field", "array<string>",
        retFields.get(9).getType());

    Assert.assertEquals("Unexpected field name of the 11th field", "map1",
        retFields.get(10).getName());
    Assert.assertEquals("Unexpected field type of the 11th field", "map<string,string>",
        retFields.get(10).getType());

    Assert.assertEquals("Unexpected field name of the 12th field", "union1",
        retFields.get(11).getName());
    Assert
        .assertEquals("Unexpected field type of the 12th field", "uniontype<float,boolean,string>",
            retFields.get(11).getType());

    Assert.assertEquals("Unexpected field name of the 14th field", "null1",
        retFields.get(13).getName());
    Assert.assertEquals("Unexpected field type of the 14th field", "void",
        retFields.get(13).getType());

    Assert.assertEquals("Unexpected field name of the 15th field", "UnionNullInt",
        retFields.get(14).getName());
    Assert.assertEquals("Unexpected field type of the 15th field", "int",
        retFields.get(14).getType());
  }

  private void createAvroTableWithExternalSchema(String dbName, String tblName, String schemaStr)
      throws TException, IOException {
    List<FieldSchema> fields = new ArrayList<>(0);
    Map<String, String> tblParams = new HashMap<>();
    tblParams.put(AvroSchemaUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), schemaStr);
    createTable(dbName, tblName, AVRO_SERIALIZATION_LIB, fields, tblParams);
  }

  @Test
  public void testSimpleTable() throws TException, IOException {
    List<FieldSchema> fields = new ArrayList<>(2);
    FieldSchema field = new FieldSchema();
    field.setName("name");
    field.setType("string");
    field.setComment("Test name comment");
    fields.add(field);

    field = new FieldSchema();
    field.setName("age");
    field.setType("int");
    field.setComment("Test age comment");
    fields.add(field);

    createTable(TEST_DB_NAME, TEST_TABLE_NAME, null, fields, null);
    List<FieldSchema> retFields = client.getFields(TEST_DB_NAME, TEST_TABLE_NAME);
    verifyTableFields(fields, retFields, null);
  }

  private void verifyTableFields(List<FieldSchema> expected, List<FieldSchema> actual,
      String nullCommentText) {
    Assert.assertEquals(expected.size(), actual.size());
    int size = expected.size();
    for (int i = 0; i < size; i++) {
      FieldSchema expectedField = expected.get(i);
      FieldSchema actualField = actual.get(i);
      Assert.assertEquals("Name does not match for field " + (i + 1), expectedField.getName(),
          actualField.getName());
      Assert.assertEquals("Type does not match for field " + (i + 1), expectedField.getType(),
          actualField.getType());
      String expectedComment = null;
      if (expectedField.getComment() == null && nullCommentText != null) {
        expectedComment = nullCommentText;
      } else {
        expectedComment = expectedField.getComment();
      }
      Assert.assertEquals("Comment does not match for field " + (i + 1), expectedComment,
          actualField.getComment());
    }
  }
}
