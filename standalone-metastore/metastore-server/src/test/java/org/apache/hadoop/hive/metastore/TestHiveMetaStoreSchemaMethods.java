/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SchemaCompatibility;
import org.apache.hadoop.hive.metastore.api.SchemaType;
import org.apache.hadoop.hive.metastore.api.SchemaValidation;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SerdeType;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.ISchemaBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SchemaVersionBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.AddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.AlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.CreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.DropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.DropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

// This does the testing using a remote metastore, as that finds more issues in thrift
@Category(MetastoreCheckinTest.class)
public class TestHiveMetaStoreSchemaMethods {
  private static Map<EventMessage.EventType, Integer> events;
  private static Map<EventMessage.EventType, Integer> transactionalEvents;
  private static Map<PreEventContext.PreEventType, Integer> preEvents;

  private static IMetaStoreClient client;
  private static Configuration conf;


  @BeforeClass
  public static void startMetastore() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setClass(conf, ConfVars.EVENT_LISTENERS, SchemaEventListener.class,
        MetaStoreEventListener.class);
    MetastoreConf.setClass(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS,
        TransactionalSchemaEventListener.class, MetaStoreEventListener.class);
    MetastoreConf.setClass(conf, ConfVars.PRE_EVENT_LISTENERS, SchemaPreEventListener.class,
        MetaStorePreEventListener.class);
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(),
        conf, false, false, false, false, false);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    client = new HiveMetaStoreClient(conf);
  }

  @Before
  public void newMaps() {
    events = new HashMap<>();
    transactionalEvents = new HashMap<>();
    preEvents = new HashMap<>();
  }

  @Test(expected = NoSuchObjectException.class)
  public void getNonExistentSchema() throws TException {
    client.getISchema(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "no.such.schema");
  }

  @Test
  public void iSchema() throws TException {
    String schemaName = uniqueSchemaName();
    String schemaGroup = "group1";
    String description = "This is a description";
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .setCompatibility(SchemaCompatibility.FORWARD)
        .setValidationLevel(SchemaValidation.LATEST)
        .setCanEvolve(false)
        .setSchemaGroup(schemaGroup)
        .setDescription(description)
        .build();
    client.createISchema(schema);

    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.CREATE_ISCHEMA));
    Assert.assertEquals(1, (int)events.get(EventMessage.EventType.CREATE_ISCHEMA));
    Assert.assertEquals(1, (int)transactionalEvents.get(EventMessage.EventType.CREATE_ISCHEMA));

    schema = client.getISchema(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName);
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.READ_ISCHEMA));

    Assert.assertEquals(SchemaType.AVRO, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(DEFAULT_CATALOG_NAME, schema.getCatName());
    Assert.assertEquals(DEFAULT_DATABASE_NAME, schema.getDbName());
    Assert.assertEquals(SchemaCompatibility.FORWARD, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.LATEST, schema.getValidationLevel());
    Assert.assertFalse(schema.isCanEvolve());
    Assert.assertEquals(schemaGroup, schema.getSchemaGroup());
    Assert.assertEquals(description, schema.getDescription());

    schemaGroup = "new group";
    description = "new description";
    schema.setCompatibility(SchemaCompatibility.BOTH);
    schema.setValidationLevel(SchemaValidation.ALL);
    schema.setCanEvolve(true);
    schema.setSchemaGroup(schemaGroup);
    schema.setDescription(description);
    client.alterISchema(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, schema);
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.ALTER_ISCHEMA));
    Assert.assertEquals(1, (int)events.get(EventMessage.EventType.ALTER_ISCHEMA));
    Assert.assertEquals(1, (int)transactionalEvents.get(EventMessage.EventType.ALTER_ISCHEMA));

    schema = client.getISchema(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName);
    Assert.assertEquals(2, (int)preEvents.get(PreEventContext.PreEventType.READ_ISCHEMA));

    Assert.assertEquals(SchemaType.AVRO, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(SchemaCompatibility.BOTH, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.ALL, schema.getValidationLevel());
    Assert.assertTrue(schema.isCanEvolve());
    Assert.assertEquals(schemaGroup, schema.getSchemaGroup());
    Assert.assertEquals(description, schema.getDescription());

    client.dropISchema(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName);
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.DROP_ISCHEMA));
    Assert.assertEquals(1, (int)events.get(EventMessage.EventType.DROP_ISCHEMA));
    Assert.assertEquals(1, (int)transactionalEvents.get(EventMessage.EventType.DROP_ISCHEMA));
    try {
      client.getISchema(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName);
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // all good
    }
  }

  @Test
  public void iSchemaOtherDatabase() throws TException {
    String catName = "other_cat";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "other_db";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, conf);

    String schemaName = uniqueSchemaName();
    String schemaGroup = "group1";
    String description = "This is a description";
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .inDb(db)
        .setCompatibility(SchemaCompatibility.FORWARD)
        .setValidationLevel(SchemaValidation.LATEST)
        .setCanEvolve(false)
        .setSchemaGroup(schemaGroup)
        .setDescription(description)
        .build();
    client.createISchema(schema);

    schema = client.getISchema(catName, dbName, schemaName);

    Assert.assertEquals(SchemaType.AVRO, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(catName, schema.getCatName());
    Assert.assertEquals(dbName, schema.getDbName());
    Assert.assertEquals(SchemaCompatibility.FORWARD, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.LATEST, schema.getValidationLevel());
    Assert.assertFalse(schema.isCanEvolve());
    Assert.assertEquals(schemaGroup, schema.getSchemaGroup());
    Assert.assertEquals(description, schema.getDescription());

    schemaGroup = "new group";
    description = "new description";
    schema.setCompatibility(SchemaCompatibility.BOTH);
    schema.setValidationLevel(SchemaValidation.ALL);
    schema.setCanEvolve(true);
    schema.setSchemaGroup(schemaGroup);
    schema.setDescription(description);
    client.alterISchema(catName, dbName, schemaName, schema);

    schema = client.getISchema(catName, dbName, schemaName);

    Assert.assertEquals(SchemaType.AVRO, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(catName, schema.getCatName());
    Assert.assertEquals(dbName, schema.getDbName());
    Assert.assertEquals(SchemaCompatibility.BOTH, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.ALL, schema.getValidationLevel());
    Assert.assertTrue(schema.isCanEvolve());
    Assert.assertEquals(schemaGroup, schema.getSchemaGroup());
    Assert.assertEquals(description, schema.getDescription());

    client.dropISchema(catName, dbName, schemaName);
    try {
      client.getISchema(catName, dbName, schemaName);
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // all good
    }
  }

  @Test(expected = NoSuchObjectException.class)
  public void schemaWithInvalidDatabase() throws TException {
    ISchema schema = new ISchemaBuilder()
        .setName("thisSchemaDoesntHaveADb")
        .setDbName("no.such.database")
        .setSchemaType(SchemaType.AVRO)
        .build();
    client.createISchema(schema);
  }

  @Test(expected = AlreadyExistsException.class)
  public void schemaAlreadyExists() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.HIVE)
        .setName(schemaName)
        .build();
    client.createISchema(schema);

    Assert.assertNotNull(schema);

    Assert.assertEquals(SchemaType.HIVE, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(SchemaCompatibility.BACKWARD, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.ALL, schema.getValidationLevel());
    Assert.assertTrue(schema.isCanEvolve());

    // This second attempt to create it should throw
    client.createISchema(schema);
  }

  @Test(expected = NoSuchObjectException.class)
  public void alterNonExistentSchema() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.HIVE)
        .setName(schemaName)
        .setDescription("a new description")
        .build();
    client.alterISchema(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, schema);
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNonExistentSchema() throws TException {
    client.dropISchema(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "no_such_schema");
  }

  @Test(expected = NoSuchObjectException.class)
  public void createVersionOfNonExistentSchema() throws TException {
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .setSchemaName("noSchemaOfThisNameExists")
        .setVersion(1)
        .addCol("a", ColumnType.STRING_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);
  }

  @Test
  public void addSchemaVersion() throws TException {
    String schemaName = uniqueSchemaName();
    int version = 1;

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);

    String description = "very descriptive";
    String schemaText = "this should look like json, but oh well";
    String fingerprint = "this should be an md5 string";
    String versionName = "why would I name a version?";
    long creationTime = 10;
    String serdeName = "serde_for_schema37";
    String serializer = "org.apache.hadoop.hive.metastore.test.Serializer";
    String deserializer = "org.apache.hadoop.hive.metastore.test.Deserializer";
    String serdeDescription = "how do you describe a serde?";
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .setCreatedAt(creationTime)
        .setState(SchemaVersionState.INITIATED)
        .setDescription(description)
        .setSchemaText(schemaText)
        .setFingerprint(fingerprint)
        .setName(versionName)
        .setSerdeName(serdeName)
        .setSerdeSerializerClass(serializer)
        .setSerdeDeserializerClass(deserializer)
        .setSerdeDescription(serdeDescription)
        .build();
    client.addSchemaVersion(schemaVersion);
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.ADD_SCHEMA_VERSION));
    Assert.assertEquals(1, (int)events.get(EventMessage.EventType.ADD_SCHEMA_VERSION));
    Assert.assertEquals(1, (int)transactionalEvents.get(EventMessage.EventType.ADD_SCHEMA_VERSION));

    schemaVersion = client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, version);
    Assert.assertNotNull(schemaVersion);
    Assert.assertEquals(schemaName, schemaVersion.getSchema().getSchemaName());
    Assert.assertEquals(DEFAULT_DATABASE_NAME, schemaVersion.getSchema().getDbName());
    Assert.assertEquals(DEFAULT_CATALOG_NAME, schemaVersion.getSchema().getCatName());
    Assert.assertEquals(version, schemaVersion.getVersion());
    Assert.assertEquals(creationTime, schemaVersion.getCreatedAt());
    Assert.assertEquals(SchemaVersionState.INITIATED, schemaVersion.getState());
    Assert.assertEquals(description, schemaVersion.getDescription());
    Assert.assertEquals(schemaText, schemaVersion.getSchemaText());
    Assert.assertEquals(fingerprint, schemaVersion.getFingerprint());
    Assert.assertEquals(versionName, schemaVersion.getName());
    Assert.assertEquals(serdeName, schemaVersion.getSerDe().getName());
    Assert.assertEquals(serializer, schemaVersion.getSerDe().getSerializerClass());
    Assert.assertEquals(deserializer, schemaVersion.getSerDe().getDeserializerClass());
    Assert.assertEquals(serdeDescription, schemaVersion.getSerDe().getDescription());
    Assert.assertEquals(2, schemaVersion.getColsSize());
    List<FieldSchema> cols = schemaVersion.getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals(ColumnType.INT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals(ColumnType.FLOAT_TYPE_NAME, cols.get(1).getType());
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.READ_SCHEMA_VERSION));

    client.dropSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, version);
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.DROP_SCHEMA_VERSION));
    Assert.assertEquals(1, (int)events.get(EventMessage.EventType.DROP_SCHEMA_VERSION));
    Assert.assertEquals(1, (int)transactionalEvents.get(EventMessage.EventType.DROP_SCHEMA_VERSION));
    try {
      client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, version);
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // all good
    }
  }

  @Test
  public void addSchemaVersionOtherDb() throws TException {
    String catName = "other_cat_for_schema_version";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "other_db_for_schema_version";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, conf);

    String schemaName = uniqueSchemaName();
    int version = 1;

    ISchema schema = new ISchemaBuilder()
        .inDb(db)
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);

    String description = "very descriptive";
    String schemaText = "this should look like json, but oh well";
    String fingerprint = "this should be an md5 string";
    String versionName = "why would I name a version?";
    long creationTime = 10;
    String serdeName = "serde_for_schema37";
    String serializer = "org.apache.hadoop.hive.metastore.test.Serializer";
    String deserializer = "org.apache.hadoop.hive.metastore.test.Deserializer";
    String serdeDescription = "how do you describe a serde?";
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .setCreatedAt(creationTime)
        .setState(SchemaVersionState.INITIATED)
        .setDescription(description)
        .setSchemaText(schemaText)
        .setFingerprint(fingerprint)
        .setName(versionName)
        .setSerdeName(serdeName)
        .setSerdeSerializerClass(serializer)
        .setSerdeDeserializerClass(deserializer)
        .setSerdeDescription(serdeDescription)
        .build();
    client.addSchemaVersion(schemaVersion);

    schemaVersion = client.getSchemaVersion(catName, dbName, schemaName, version);
    Assert.assertNotNull(schemaVersion);
    Assert.assertEquals(schemaName, schemaVersion.getSchema().getSchemaName());
    Assert.assertEquals(dbName, schemaVersion.getSchema().getDbName());
    Assert.assertEquals(catName, schemaVersion.getSchema().getCatName());
    Assert.assertEquals(version, schemaVersion.getVersion());
    Assert.assertEquals(creationTime, schemaVersion.getCreatedAt());
    Assert.assertEquals(SchemaVersionState.INITIATED, schemaVersion.getState());
    Assert.assertEquals(description, schemaVersion.getDescription());
    Assert.assertEquals(schemaText, schemaVersion.getSchemaText());
    Assert.assertEquals(fingerprint, schemaVersion.getFingerprint());
    Assert.assertEquals(versionName, schemaVersion.getName());
    Assert.assertEquals(serdeName, schemaVersion.getSerDe().getName());
    Assert.assertEquals(serializer, schemaVersion.getSerDe().getSerializerClass());
    Assert.assertEquals(deserializer, schemaVersion.getSerDe().getDeserializerClass());
    Assert.assertEquals(serdeDescription, schemaVersion.getSerDe().getDescription());
    Assert.assertEquals(2, schemaVersion.getColsSize());
    List<FieldSchema> cols = schemaVersion.getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals(ColumnType.INT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals(ColumnType.FLOAT_TYPE_NAME, cols.get(1).getType());
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.READ_SCHEMA_VERSION));

    client.dropSchemaVersion(catName, dbName, schemaName, version);
    try {
      client.getSchemaVersion(catName, dbName, schemaName, version);
      Assert.fail();
    } catch (NoSuchObjectException e) {
      // all good
    }
  }

  // Test that adding multiple versions of the same schema
  @Test
  public void multipleSchemaVersions() throws TException {
    String schemaName = uniqueSchemaName();

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(1)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);

    schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(2)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .addCol("b", ColumnType.DATE_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);

    schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(3)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .addCol("b", ColumnType.DATE_TYPE_NAME)
        .addCol("c", ColumnType.TIMESTAMP_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);
    Assert.assertEquals(3, (int)preEvents.get(PreEventContext.PreEventType.ADD_SCHEMA_VERSION));
    Assert.assertEquals(3, (int)events.get(EventMessage.EventType.ADD_SCHEMA_VERSION));
    Assert.assertEquals(3, (int)transactionalEvents.get(EventMessage.EventType.ADD_SCHEMA_VERSION));

    schemaVersion = client.getSchemaLatestVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName);
    Assert.assertEquals(3, schemaVersion.getVersion());
    Assert.assertEquals(3, schemaVersion.getColsSize());
    List<FieldSchema> cols = schemaVersion.getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals("c", cols.get(2).getName());
    Assert.assertEquals(ColumnType.BIGINT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals(ColumnType.DATE_TYPE_NAME, cols.get(1).getType());
    Assert.assertEquals(ColumnType.TIMESTAMP_TYPE_NAME, cols.get(2).getType());
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.READ_SCHEMA_VERSION));

    List<SchemaVersion> versions = client.getSchemaAllVersions(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName);
    Assert.assertEquals(2, (int)preEvents.get(PreEventContext.PreEventType.READ_SCHEMA_VERSION));
    Assert.assertEquals(3, versions.size());
    versions.sort(Comparator.comparingInt(SchemaVersion::getVersion));
    Assert.assertEquals(1, versions.get(0).getVersion());
    Assert.assertEquals(1, versions.get(0).getColsSize());
    Assert.assertEquals(ColumnType.BIGINT_TYPE_NAME, versions.get(0).getCols().get(0).getType());

    Assert.assertEquals(2, versions.get(1).getVersion());
    Assert.assertEquals(2, versions.get(1).getColsSize());
    cols = versions.get(1).getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals(ColumnType.BIGINT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals(ColumnType.DATE_TYPE_NAME, cols.get(1).getType());

    Assert.assertEquals(3, versions.get(2).getVersion());
    Assert.assertEquals(3, versions.get(2).getColsSize());
    cols = versions.get(2).getCols();
    Collections.sort(cols);
    Assert.assertEquals("a", cols.get(0).getName());
    Assert.assertEquals("b", cols.get(1).getName());
    Assert.assertEquals("c", cols.get(2).getName());
    Assert.assertEquals(ColumnType.BIGINT_TYPE_NAME, cols.get(0).getType());
    Assert.assertEquals(ColumnType.DATE_TYPE_NAME, cols.get(1).getType());
    Assert.assertEquals(ColumnType.TIMESTAMP_TYPE_NAME, cols.get(2).getType());
  }

  @Test(expected = NoSuchObjectException.class)
  public void nonExistentSchemaVersion() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, 1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void schemaVersionBogusDb() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaVersion(DEFAULT_CATALOG_NAME, "bogus", schemaName, 1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void schemaVersionBogusCatalog() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaVersion("bogus", DEFAULT_DATABASE_NAME, schemaName, 1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void nonExistentSchemaVersionButOtherVersionsExist() throws TException {
    String schemaName = uniqueSchemaName();

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);

    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(1)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);

    client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, 2);
  }

  @Test(expected = NoSuchObjectException.class)
  public void getLatestSchemaButNoVersions() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaLatestVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName);
  }

  @Test(expected = NoSuchObjectException.class)
  public void getLatestSchemaNoSuchSchema() throws TException {
    client.getSchemaLatestVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "no.such.schema.with.this.name");
  }

  @Test(expected = NoSuchObjectException.class)
  public void latestSchemaVersionBogusDb() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaLatestVersion(DEFAULT_CATALOG_NAME, "bogus", schemaName);
  }

  @Test(expected = NoSuchObjectException.class)
  public void latestSchemaVersionBogusCatalog() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaLatestVersion("bogus", DEFAULT_DATABASE_NAME, schemaName);
  }

  @Test(expected = NoSuchObjectException.class)
  public void getAllSchemaButNoVersions() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaAllVersions(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName);
  }

  @Test(expected = NoSuchObjectException.class)
  public void getAllSchemaNoSuchSchema() throws TException {
    client.getSchemaAllVersions(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "no.such.schema.with.this.name");
  }

  @Test(expected = NoSuchObjectException.class)
  public void allSchemaVersionBogusDb() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaAllVersions(DEFAULT_CATALOG_NAME, "bogus", schemaName);
  }

  @Test(expected = NoSuchObjectException.class)
  public void allSchemaVersionBogusCatalog() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.getSchemaAllVersions("bogus", DEFAULT_DATABASE_NAME, schemaName);
  }

  @Test(expected = AlreadyExistsException.class)
  public void addDuplicateSchemaVersion() throws TException {
    String schemaName = uniqueSchemaName();
    int version = 1;

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);

    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);

    client.addSchemaVersion(schemaVersion);
  }

  @Test(expected = NoSuchObjectException.class)
  public void mapSerDeNoSuchSchema() throws TException {
    SerDeInfo serDeInfo = new SerDeInfo(uniqueSerdeName(), "lib", Collections.emptyMap());
    client.mapSchemaVersionToSerde(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, uniqueSchemaName(), 1, serDeInfo.getName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void mapSerDeNoSuchSchemaVersion() throws TException {
    SerDeInfo serDeInfo = new SerDeInfo(uniqueSerdeName(), "lib", Collections.emptyMap());
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(uniqueSchemaName())
        .build();
    client.createISchema(schema);
    client.mapSchemaVersionToSerde(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schema.getName(), 3, serDeInfo.getName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void mapNonExistentSerdeToSchemaVersion() throws TException {
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(uniqueSchemaName())
        .build();
    client.createISchema(schema);

    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(1)
        .addCol("x", ColumnType.BOOLEAN_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);
    client.mapSchemaVersionToSerde(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schema.getName(), schemaVersion.getVersion(), uniqueSerdeName());
  }

  @Test
  public void mapSerdeToSchemaVersion() throws TException {
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(uniqueSchemaName())
        .build();
    client.createISchema(schema);

    // Create schema with no serde, then map it
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(1)
        .addCol("x", ColumnType.BOOLEAN_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);

    SerDeInfo serDeInfo = new SerDeInfo(uniqueSerdeName(), "lib", Collections.emptyMap());
    client.addSerDe(serDeInfo);

    client.mapSchemaVersionToSerde(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schema.getName(), schemaVersion.getVersion(), serDeInfo.getName());
    schemaVersion = client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schema.getName(), schemaVersion.getVersion());
    Assert.assertEquals(serDeInfo.getName(), schemaVersion.getSerDe().getName());

    // Create schema with a serde, then remap it
    String serDeName = uniqueSerdeName();
    schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(2)
        .addCol("x", ColumnType.BOOLEAN_TYPE_NAME)
        .setSerdeName(serDeName)
        .setSerdeLib("x")
        .build();
    client.addSchemaVersion(schemaVersion);

    schemaVersion = client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schema.getName(), 2);
    Assert.assertEquals(serDeName, schemaVersion.getSerDe().getName());

    serDeInfo = new SerDeInfo(uniqueSerdeName(), "y", Collections.emptyMap());
    client.addSerDe(serDeInfo);
    client.mapSchemaVersionToSerde(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schema.getName(), 2, serDeInfo.getName());
    schemaVersion = client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schema.getName(), 2);
    Assert.assertEquals(serDeInfo.getName(), schemaVersion.getSerDe().getName());
  }

  @Test
  public void mapSerdeToSchemaVersionOtherDb() throws TException {
    String catName = "other_cat_for_map_to";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "map_other_db";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, conf);

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .inDb(db)
        .setName(uniqueSchemaName())
        .build();
    client.createISchema(schema);

    // Create schema with no serde, then map it
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(1)
        .addCol("x", ColumnType.BOOLEAN_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);

    SerDeInfo serDeInfo = new SerDeInfo(uniqueSerdeName(), "lib", Collections.emptyMap());
    client.addSerDe(serDeInfo);

    client.mapSchemaVersionToSerde(catName, dbName, schema.getName(), schemaVersion.getVersion(), serDeInfo.getName());
    schemaVersion = client.getSchemaVersion(catName, dbName, schema.getName(), schemaVersion.getVersion());
    Assert.assertEquals(serDeInfo.getName(), schemaVersion.getSerDe().getName());

    // Create schema with a serde, then remap it
    String serDeName = uniqueSerdeName();
    schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(2)
        .addCol("x", ColumnType.BOOLEAN_TYPE_NAME)
        .setSerdeName(serDeName)
        .setSerdeLib("x")
        .build();
    client.addSchemaVersion(schemaVersion);

    schemaVersion = client.getSchemaVersion(catName, dbName, schema.getName(), 2);
    Assert.assertEquals(serDeName, schemaVersion.getSerDe().getName());

    serDeInfo = new SerDeInfo(uniqueSerdeName(), "y", Collections.emptyMap());
    client.addSerDe(serDeInfo);
    client.mapSchemaVersionToSerde(catName, dbName, schema.getName(), 2, serDeInfo.getName());
    schemaVersion = client.getSchemaVersion(catName, dbName, schema.getName(), 2);
    Assert.assertEquals(serDeInfo.getName(), schemaVersion.getSerDe().getName());

  }

  @Test
  public void addSerde() throws TException {
    String serdeName = uniqueSerdeName();
    SerDeInfo serDeInfo = new SerDeInfo(serdeName, "serdeLib", Collections.singletonMap("a", "b"));
    serDeInfo.setSerializerClass("serializer");
    serDeInfo.setDeserializerClass("deserializer");
    serDeInfo.setDescription("description");
    serDeInfo.setSerdeType(SerdeType.SCHEMA_REGISTRY);
    client.addSerDe(serDeInfo);

    serDeInfo = client.getSerDe(serdeName);
    Assert.assertEquals(serdeName, serDeInfo.getName());
    Assert.assertEquals("serdeLib", serDeInfo.getSerializationLib());
    Assert.assertEquals(1, serDeInfo.getParametersSize());
    Assert.assertEquals("b", serDeInfo.getParameters().get("a"));
    Assert.assertEquals("serializer", serDeInfo.getSerializerClass());
    Assert.assertEquals("deserializer", serDeInfo.getDeserializerClass());
    Assert.assertEquals("description", serDeInfo.getDescription());
    Assert.assertEquals(SerdeType.SCHEMA_REGISTRY, serDeInfo.getSerdeType());
  }

  @Test(expected = AlreadyExistsException.class)
  public void duplicateSerde() throws TException {
    String serdeName = uniqueSerdeName();
    SerDeInfo serDeInfo = new SerDeInfo(serdeName, "x", Collections.emptyMap());
    client.addSerDe(serDeInfo);
    client.addSerDe(serDeInfo);
  }

  @Test(expected = NoSuchObjectException.class)
  public void noSuchSerDe() throws TException {
    client.getSerDe(uniqueSerdeName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void setVersionStateNoSuchSchema() throws TException {
    client.setSchemaVersionState(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "no.such.schema", 1, SchemaVersionState.INITIATED);
  }

  @Test(expected = NoSuchObjectException.class)
  public void setVersionStateNoSuchVersion() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);
    client.setSchemaVersionState(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, 1, SchemaVersionState.INITIATED);
  }

  @Test
  public void setVersionState() throws TException {
    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .build();
    client.createISchema(schema);

    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(1)
        .addCol("a", ColumnType.BINARY_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);

    schemaVersion = client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, 1);
    Assert.assertNull(schemaVersion.getState());

    client.setSchemaVersionState(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, 1, SchemaVersionState.INITIATED);
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.ALTER_SCHEMA_VERSION));
    Assert.assertEquals(1, (int)events.get(EventMessage.EventType.ALTER_SCHEMA_VERSION));
    Assert.assertEquals(1, (int)transactionalEvents.get(EventMessage.EventType.ALTER_SCHEMA_VERSION));
    schemaVersion = client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, 1);
    Assert.assertEquals(SchemaVersionState.INITIATED, schemaVersion.getState());

    client.setSchemaVersionState(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, 1, SchemaVersionState.REVIEWED);
    Assert.assertEquals(2, (int)preEvents.get(PreEventContext.PreEventType.ALTER_SCHEMA_VERSION));
    Assert.assertEquals(2, (int)events.get(EventMessage.EventType.ALTER_SCHEMA_VERSION));
    Assert.assertEquals(2, (int)transactionalEvents.get(EventMessage.EventType.ALTER_SCHEMA_VERSION));
    schemaVersion = client.getSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName, 1);
    Assert.assertEquals(SchemaVersionState.REVIEWED, schemaVersion.getState());
  }

  @Test
  public void setVersionStateOtherDb() throws TException {
    String catName = "other_cat_for_set_version";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "other_db_set_state";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, conf);

    String schemaName = uniqueSchemaName();
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .inDb(db)
        .build();
    client.createISchema(schema);

    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(1)
        .addCol("a", ColumnType.BINARY_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion);

    schemaVersion = client.getSchemaVersion(catName, dbName, schemaName, 1);
    Assert.assertNull(schemaVersion.getState());

    client.setSchemaVersionState(catName, dbName, schemaName, 1, SchemaVersionState.INITIATED);
    Assert.assertEquals(1, (int)preEvents.get(PreEventContext.PreEventType.ALTER_SCHEMA_VERSION));
    Assert.assertEquals(1, (int)events.get(EventMessage.EventType.ALTER_SCHEMA_VERSION));
    Assert.assertEquals(1, (int)transactionalEvents.get(EventMessage.EventType.ALTER_SCHEMA_VERSION));
    schemaVersion = client.getSchemaVersion(catName, dbName, schemaName, 1);
    Assert.assertEquals(SchemaVersionState.INITIATED, schemaVersion.getState());

    client.setSchemaVersionState(catName, dbName, schemaName, 1, SchemaVersionState.REVIEWED);
    Assert.assertEquals(2, (int)preEvents.get(PreEventContext.PreEventType.ALTER_SCHEMA_VERSION));
    Assert.assertEquals(2, (int)events.get(EventMessage.EventType.ALTER_SCHEMA_VERSION));
    Assert.assertEquals(2, (int)transactionalEvents.get(EventMessage.EventType.ALTER_SCHEMA_VERSION));
    schemaVersion = client.getSchemaVersion(catName, dbName, schemaName, 1);
    Assert.assertEquals(SchemaVersionState.REVIEWED, schemaVersion.getState());
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNonExistentSchemaVersion() throws TException {
    client.dropSchemaVersion(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "ther is no schema named this", 23);
  }

  @Test
  public void schemaQuery() throws TException {
    String dbName = "schema_query_db";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);

    String schemaName1 = uniqueSchemaName();
    ISchema schema1 = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setDbName(dbName)
        .setName(schemaName1)
        .build();
    client.createISchema(schema1);

    String schemaName2 = uniqueSchemaName();
    ISchema schema2 = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setDbName(dbName)
        .setName(schemaName2)
        .build();
    client.createISchema(schema2);

    SchemaVersion schemaVersion1_1 = new SchemaVersionBuilder()
        .versionOf(schema1)
        .setVersion(1)
        .addCol("alpha", ColumnType.BIGINT_TYPE_NAME)
        .addCol("beta", ColumnType.DATE_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion1_1);

    SchemaVersion schemaVersion1_2 = new SchemaVersionBuilder()
        .versionOf(schema1)
        .setVersion(2)
        .addCol("alpha", ColumnType.BIGINT_TYPE_NAME)
        .addCol("beta", ColumnType.DATE_TYPE_NAME)
        .addCol("gamma", ColumnType.BIGINT_TYPE_NAME, "namespace=x")
        .build();
    client.addSchemaVersion(schemaVersion1_2);

    SchemaVersion schemaVersion2_1 = new SchemaVersionBuilder()
        .versionOf(schema2)
        .setVersion(1)
        .addCol("ALPHA", ColumnType.SMALLINT_TYPE_NAME)
        .addCol("delta", ColumnType.DOUBLE_TYPE_NAME)
        .build();
    client.addSchemaVersion(schemaVersion2_1);

    SchemaVersion schemaVersion2_2 = new SchemaVersionBuilder()
        .versionOf(schema2)
        .setVersion(2)
        .addCol("ALPHA", ColumnType.SMALLINT_TYPE_NAME)
        .addCol("delta", ColumnType.DOUBLE_TYPE_NAME)
        .addCol("epsilon", ColumnType.STRING_TYPE_NAME, "namespace=x")
        .build();
    client.addSchemaVersion(schemaVersion2_2);

    // Query that should return nothing
    FindSchemasByColsRqst rqst = new FindSchemasByColsRqst();
    rqst.setColName("x");
    rqst.setColNamespace("y");
    rqst.setType("z");
    FindSchemasByColsResp rsp = client.getSchemaByCols(rqst);
    Assert.assertEquals(0, rsp.getSchemaVersionsSize());

    // Query that should fetch one column
    rqst = new FindSchemasByColsRqst();
    rqst.setColName("gamma");
    rsp = client.getSchemaByCols(rqst);
    Assert.assertEquals(1, rsp.getSchemaVersionsSize());
    Assert.assertEquals(schemaName1, rsp.getSchemaVersions().get(0).getSchema().getSchemaName());
    Assert.assertEquals(dbName, rsp.getSchemaVersions().get(0).getSchema().getDbName());
    Assert.assertEquals(2, rsp.getSchemaVersions().get(0).getVersion());

    // fetch 2 in same schema
    rqst = new FindSchemasByColsRqst();
    rqst.setColName("beta");
    rsp = client.getSchemaByCols(rqst);
    Assert.assertEquals(2, rsp.getSchemaVersionsSize());
    List<SchemaVersionDescriptor> results = new ArrayList<>(rsp.getSchemaVersions());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(0).getSchema().getDbName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName1, results.get(1).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(1).getSchema().getDbName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // fetch across schemas
    rqst = new FindSchemasByColsRqst();
    rqst.setColName("alpha");
    rsp = client.getSchemaByCols(rqst);
    Assert.assertEquals(4, rsp.getSchemaVersionsSize());
    results = new ArrayList<>(rsp.getSchemaVersions());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(0).getSchema().getDbName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName1, results.get(1).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(1).getSchema().getDbName());
    Assert.assertEquals(2, results.get(1).getVersion());
    Assert.assertEquals(schemaName2, results.get(2).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(2).getSchema().getDbName());
    Assert.assertEquals(1, results.get(2).getVersion());
    Assert.assertEquals(schemaName2, results.get(3).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(3).getSchema().getDbName());
    Assert.assertEquals(2, results.get(3).getVersion());

    // fetch by namespace
    rqst = new FindSchemasByColsRqst();
    rqst.setColNamespace("namespace=x");
    rsp = client.getSchemaByCols(rqst);
    Assert.assertEquals(2, rsp.getSchemaVersionsSize());
    results = new ArrayList<>(rsp.getSchemaVersions());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(0).getSchema().getDbName());
    Assert.assertEquals(2, results.get(0).getVersion());
    Assert.assertEquals(schemaName2, results.get(1).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(1).getSchema().getDbName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // fetch by name and type
    rqst = new FindSchemasByColsRqst();
    rqst.setColName("alpha");
    rqst.setType(ColumnType.SMALLINT_TYPE_NAME);
    rsp = client.getSchemaByCols(rqst);
    Assert.assertEquals(2, rsp.getSchemaVersionsSize());
    results = new ArrayList<>(rsp.getSchemaVersions());
    Collections.sort(results);
    Assert.assertEquals(schemaName2, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(0).getSchema().getDbName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName2, results.get(1).getSchema().getSchemaName());
    Assert.assertEquals(dbName, results.get(1).getSchema().getDbName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // Make sure matching name but wrong type doesn't return
    rqst = new FindSchemasByColsRqst();
    rqst.setColName("alpha");
    rqst.setType(ColumnType.STRING_TYPE_NAME);
    rsp = client.getSchemaByCols(rqst);
    Assert.assertEquals(0, rsp.getSchemaVersionsSize());
  }

  @Test(expected = MetaException.class)
  public void schemaVersionQueryNoNameOrNamespace() throws TException {
    FindSchemasByColsRqst rqst = new FindSchemasByColsRqst();
    rqst.setType(ColumnType.STRING_TYPE_NAME);
    client.getSchemaByCols(rqst);
  }

  private static int nextSchemaNum = 1;

  private String uniqueSchemaName() {
    return "uniqueschema" + nextSchemaNum++;

  }

  private String uniqueSerdeName() {
    return "uniqueSerde" + nextSchemaNum++;
  }

  public static class SchemaEventListener extends MetaStoreEventListener {

    public SchemaEventListener(Configuration config) {
      super(config);
    }

    @Override
    public void onCreateISchema(CreateISchemaEvent createISchemaEvent) throws MetaException {
      Integer cnt = events.get(EventMessage.EventType.CREATE_ISCHEMA);
      events.put(EventMessage.EventType.CREATE_ISCHEMA, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onAlterISchema(AlterISchemaEvent alterISchemaEvent) throws MetaException {
      Integer cnt = events.get(EventMessage.EventType.ALTER_ISCHEMA);
      events.put(EventMessage.EventType.ALTER_ISCHEMA, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onDropISchema(DropISchemaEvent dropISchemaEvent) throws MetaException {
      Integer cnt = events.get(EventMessage.EventType.DROP_ISCHEMA);
      events.put(EventMessage.EventType.DROP_ISCHEMA, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onAddSchemaVersion(AddSchemaVersionEvent addSchemaVersionEvent) throws
        MetaException {
      Integer cnt = events.get(EventMessage.EventType.ADD_SCHEMA_VERSION);
      events.put(EventMessage.EventType.ADD_SCHEMA_VERSION, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onAlterSchemaVersion(AlterSchemaVersionEvent alterSchemaVersionEvent) throws
        MetaException {
      Integer cnt = events.get(EventMessage.EventType.ALTER_SCHEMA_VERSION);
      events.put(EventMessage.EventType.ALTER_SCHEMA_VERSION, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onDropSchemaVersion(DropSchemaVersionEvent dropSchemaVersionEvent) throws
        MetaException {
      Integer cnt = events.get(EventMessage.EventType.DROP_SCHEMA_VERSION);
      events.put(EventMessage.EventType.DROP_SCHEMA_VERSION, cnt == null ? 1 : cnt + 1);
    }
  }

  public static class TransactionalSchemaEventListener extends MetaStoreEventListener {

    public TransactionalSchemaEventListener(Configuration config) {
      super(config);
    }

    @Override
    public void onCreateISchema(CreateISchemaEvent createISchemaEvent) throws MetaException {
      Integer cnt = transactionalEvents.get(EventMessage.EventType.CREATE_ISCHEMA);
      transactionalEvents.put(EventMessage.EventType.CREATE_ISCHEMA, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onAlterISchema(AlterISchemaEvent alterISchemaEvent) throws MetaException {
      Integer cnt = transactionalEvents.get(EventMessage.EventType.ALTER_ISCHEMA);
      transactionalEvents.put(EventMessage.EventType.ALTER_ISCHEMA, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onDropISchema(DropISchemaEvent dropISchemaEvent) throws MetaException {
      Integer cnt = transactionalEvents.get(EventMessage.EventType.DROP_ISCHEMA);
      transactionalEvents.put(EventMessage.EventType.DROP_ISCHEMA, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onAddSchemaVersion(AddSchemaVersionEvent addSchemaVersionEvent) throws
        MetaException {
      Integer cnt = transactionalEvents.get(EventMessage.EventType.ADD_SCHEMA_VERSION);
      transactionalEvents.put(EventMessage.EventType.ADD_SCHEMA_VERSION, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onAlterSchemaVersion(AlterSchemaVersionEvent alterSchemaVersionEvent) throws
        MetaException {
      Integer cnt = transactionalEvents.get(EventMessage.EventType.ALTER_SCHEMA_VERSION);
      transactionalEvents.put(EventMessage.EventType.ALTER_SCHEMA_VERSION, cnt == null ? 1 : cnt + 1);
    }

    @Override
    public void onDropSchemaVersion(DropSchemaVersionEvent dropSchemaVersionEvent) throws
        MetaException {
      Integer cnt = transactionalEvents.get(EventMessage.EventType.DROP_SCHEMA_VERSION);
      transactionalEvents.put(EventMessage.EventType.DROP_SCHEMA_VERSION, cnt == null ? 1 : cnt + 1);
    }
  }

  public static class SchemaPreEventListener extends MetaStorePreEventListener {

    public SchemaPreEventListener(Configuration config) {
      super(config);
    }

    @Override
    public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException,
        InvalidOperationException {
      Integer cnt = preEvents.get(context.getEventType());
      preEvents.put(context.getEventType(), cnt == null ? 1 : cnt + 1);

    }
  }


}
