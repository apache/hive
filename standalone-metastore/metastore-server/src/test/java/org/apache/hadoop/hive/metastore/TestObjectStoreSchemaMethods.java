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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SchemaCompatibility;
import org.apache.hadoop.hive.metastore.api.SchemaType;
import org.apache.hadoop.hive.metastore.api.SchemaValidation;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.ISchemaBuilder;
import org.apache.hadoop.hive.metastore.client.builder.SchemaVersionBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

@Category(MetastoreCheckinTest.class)
public class TestObjectStoreSchemaMethods {
  private RawStore objectStore;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
        DefaultPartitionExpressionProxy.class.getName());

    objectStore = new ObjectStore();
    objectStore.setConf(conf);
  }

  @Test
  public void iSchema() throws TException {
    Database db = createUniqueDatabaseForTest();
    ISchema schema = objectStore.getISchema(new ISchemaName(db.getCatalogName(), db.getName(), "no.such.schema"));
    Assert.assertNull(schema);

    String schemaName = "schema1";
    String schemaGroup = "group1";
    String description = "This is a description";
    schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .inDb(db)
        .setCompatibility(SchemaCompatibility.FORWARD)
        .setValidationLevel(SchemaValidation.LATEST)
        .setCanEvolve(false)
        .setSchemaGroup(schemaGroup)
        .setDescription(description)
        .build();
    objectStore.createISchema(schema);

    schema = objectStore.getISchema(new ISchemaName(db.getCatalogName(), db.getName(), schemaName));
    Assert.assertNotNull(schema);

    Assert.assertEquals(SchemaType.AVRO, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
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
    objectStore.alterISchema(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), schema);

    schema = objectStore.getISchema(new ISchemaName(db.getCatalogName(), db.getName(), schemaName));
    Assert.assertNotNull(schema);

    Assert.assertEquals(SchemaType.AVRO, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(SchemaCompatibility.BOTH, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.ALL, schema.getValidationLevel());
    Assert.assertTrue(schema.isCanEvolve());
    Assert.assertEquals(schemaGroup, schema.getSchemaGroup());
    Assert.assertEquals(description, schema.getDescription());

    objectStore.dropISchema(new ISchemaName(db.getCatalogName(), db.getName(), schemaName));
    schema = objectStore.getISchema(new ISchemaName(db.getCatalogName(), db.getName(), schemaName));
    Assert.assertNull(schema);
  }

  @Test(expected = NoSuchObjectException.class)
  public void schemaWithInvalidDatabase() throws MetaException, AlreadyExistsException,
      NoSuchObjectException {
    ISchema schema = new ISchemaBuilder()
        .setName("thisSchemaDoesntHaveADb")
        .setDbName("no.such.database")
        .setSchemaType(SchemaType.AVRO)
        .build();
    objectStore.createISchema(schema);
  }

  @Test(expected = AlreadyExistsException.class)
  public void schemaAlreadyExists() throws TException {
    Database db = createUniqueDatabaseForTest();
    String schemaName = "schema2";
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.HIVE)
        .setName(schemaName)
        .inDb(db)
        .build();
    objectStore.createISchema(schema);

    schema = objectStore.getISchema(new ISchemaName(db.getCatalogName(), db.getName(), schemaName));
    Assert.assertNotNull(schema);

    Assert.assertEquals(SchemaType.HIVE, schema.getSchemaType());
    Assert.assertEquals(schemaName, schema.getName());
    Assert.assertEquals(SchemaCompatibility.BACKWARD, schema.getCompatibility());
    Assert.assertEquals(SchemaValidation.ALL, schema.getValidationLevel());
    Assert.assertTrue(schema.isCanEvolve());

    // This second attempt to create it should throw
    objectStore.createISchema(schema);
  }

  @Test(expected = NoSuchObjectException.class)
  public void alterNonExistentSchema() throws MetaException, NoSuchObjectException {
    String schemaName = "noSuchSchema";
    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.HIVE)
        .setName(schemaName)
        .setDescription("a new description")
        .build();
    objectStore.alterISchema(new ISchemaName(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName), schema);
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNonExistentSchema() throws MetaException, NoSuchObjectException {
    objectStore.dropISchema(new ISchemaName(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "no_such_schema"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void createVersionOfNonExistentSchema() throws MetaException, AlreadyExistsException,
      NoSuchObjectException, InvalidObjectException {
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .setSchemaName("noSchemaOfThisNameExists")
        .setVersion(1)
        .addCol("a", ColumnType.STRING_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);
  }

  @Test
  public void addSchemaVersion() throws TException {
    Database db = createUniqueDatabaseForTest();
    String schemaName = "schema37";
    int version = 1;
    SchemaVersion schemaVersion = objectStore.getSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version));
    Assert.assertNull(schemaVersion);

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .inDb(db)
        .build();
    objectStore.createISchema(schema);

    String description = "very descriptive";
    String schemaText = "this should look like json, but oh well";
    String fingerprint = "this should be an md5 string";
    String versionName = "why would I name a version?";
    long creationTime = 10;
    String serdeName = "serde_for_schema37";
    String serializer = "org.apache.hadoop.hive.metastore.test.Serializer";
    String deserializer = "org.apache.hadoop.hive.metastore.test.Deserializer";
    String serdeDescription = "how do you describe a serde?";
    schemaVersion = new SchemaVersionBuilder()
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
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = objectStore.getSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version));
    Assert.assertNotNull(schemaVersion);
    Assert.assertEquals(schemaName, schemaVersion.getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), schemaVersion.getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), schemaVersion.getSchema().getCatName());
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

    objectStore.dropSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version));
    schemaVersion = objectStore.getSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version));
    Assert.assertNull(schemaVersion);
  }

  // Test that adding multiple versions of the same schema
  @Test
  public void multipleSchemaVersions() throws TException {
    Database db = createUniqueDatabaseForTest();
    String schemaName = "schema195";

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .inDb(db)
        .build();
    objectStore.createISchema(schema);
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(1)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(2)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .addCol("b", ColumnType.DATE_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(3)
        .addCol("a", ColumnType.BIGINT_TYPE_NAME)
        .addCol("b", ColumnType.DATE_TYPE_NAME)
        .addCol("c", ColumnType.TIMESTAMP_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = objectStore.getLatestSchemaVersion(new ISchemaName(db.getCatalogName(), db.getName(), schemaName));
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

    schemaVersion = objectStore.getLatestSchemaVersion(new ISchemaName(db.getCatalogName(), db.getName(), "no.such.schema.with.this.name"));
    Assert.assertNull(schemaVersion);

    List<SchemaVersion> versions =
        objectStore.getAllSchemaVersion(new ISchemaName(db.getCatalogName(), db.getName(), "there.really.isnt.a.schema.named.this"));
    Assert.assertNull(versions);

    versions = objectStore.getAllSchemaVersion(new ISchemaName(db.getCatalogName(), db.getName(), schemaName));
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

  @Test(expected = AlreadyExistsException.class)
  public void addDuplicateSchemaVersion() throws TException {
    Database db = createUniqueDatabaseForTest();
    String schemaName = "schema1234";
    int version = 1;
    SchemaVersion schemaVersion = objectStore.getSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version));
    Assert.assertNull(schemaVersion);

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .inDb(db)
        .build();
    objectStore.createISchema(schema);

    schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    objectStore.addSchemaVersion(schemaVersion);
  }

  @Test
  public void alterSchemaVersion() throws TException {
    Database db = createUniqueDatabaseForTest();
    String schemaName = "schema371234";
    int version = 1;
    SchemaVersion schemaVersion = objectStore.getSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version));
    Assert.assertNull(schemaVersion);

    ISchema schema = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName)
        .inDb(db)
        .build();
    objectStore.createISchema(schema);

    schemaVersion = new SchemaVersionBuilder()
        .versionOf(schema)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .setState(SchemaVersionState.INITIATED)
        .build();
    objectStore.addSchemaVersion(schemaVersion);

    schemaVersion = objectStore.getSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version));
    Assert.assertNotNull(schemaVersion);
    Assert.assertEquals(schemaName, schemaVersion.getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), schemaVersion.getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), schemaVersion.getSchema().getCatName());
    Assert.assertEquals(version, schemaVersion.getVersion());
    Assert.assertEquals(SchemaVersionState.INITIATED, schemaVersion.getState());

    schemaVersion.setState(SchemaVersionState.REVIEWED);
    String serdeName = "serde for " + schemaName;
    SerDeInfo serde = new SerDeInfo(serdeName, "", Collections.emptyMap());
    String serializer = "org.apache.hadoop.hive.metastore.test.Serializer";
    String deserializer = "org.apache.hadoop.hive.metastore.test.Deserializer";
    serde.setSerializerClass(serializer);
    serde.setDeserializerClass(deserializer);
    schemaVersion.setSerDe(serde);
    objectStore.alterSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version), schemaVersion);

    schemaVersion = objectStore.getSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(db.getCatalogName(), db.getName(), schemaName), version));
    Assert.assertNotNull(schemaVersion);
    Assert.assertEquals(schemaName, schemaVersion.getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), schemaVersion.getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), schemaVersion.getSchema().getCatName());
    Assert.assertEquals(version, schemaVersion.getVersion());
    Assert.assertEquals(SchemaVersionState.REVIEWED, schemaVersion.getState());
    Assert.assertEquals(serdeName, schemaVersion.getSerDe().getName());
    Assert.assertEquals(serializer, schemaVersion.getSerDe().getSerializerClass());
    Assert.assertEquals(deserializer, schemaVersion.getSerDe().getDeserializerClass());
  }

  @Test(expected = NoSuchObjectException.class)
  public void alterNonExistentSchemaVersion() throws MetaException, AlreadyExistsException,
      NoSuchObjectException {
    String schemaName = "schema3723asdflj";
    int version = 37;
    SchemaVersion schemaVersion = new SchemaVersionBuilder()
        .setSchemaName(schemaName)
        .setDbName(DEFAULT_DATABASE_NAME)
        .setVersion(version)
        .addCol("a", ColumnType.INT_TYPE_NAME)
        .addCol("b", ColumnType.FLOAT_TYPE_NAME)
        .setState(SchemaVersionState.INITIATED)
        .build();
    objectStore.alterSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, schemaName), version), schemaVersion);
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropNonExistentSchemaVersion() throws NoSuchObjectException, MetaException {
    objectStore.dropSchemaVersion(new SchemaVersionDescriptor(new ISchemaName(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME, "ther is no schema named this"), 23));
  }

  @Test
  public void schemaQuery() throws TException {
    Database db = createUniqueDatabaseForTest();
    String schemaName1 = "a_schema1";
    ISchema schema1 = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName1)
        .inDb(db)
        .build();
    objectStore.createISchema(schema1);

    String schemaName2 = "a_schema2";
    ISchema schema2 = new ISchemaBuilder()
        .setSchemaType(SchemaType.AVRO)
        .setName(schemaName2)
        .inDb(db)
        .build();
    objectStore.createISchema(schema2);

    SchemaVersion schemaVersion1_1 = new SchemaVersionBuilder()
        .versionOf(schema1)
        .setVersion(1)
        .addCol("alpha", ColumnType.BIGINT_TYPE_NAME)
        .addCol("beta", ColumnType.DATE_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion1_1);

    SchemaVersion schemaVersion1_2 = new SchemaVersionBuilder()
        .versionOf(schema1)
        .setVersion(2)
        .addCol("alpha", ColumnType.BIGINT_TYPE_NAME)
        .addCol("beta", ColumnType.DATE_TYPE_NAME)
        .addCol("gamma", ColumnType.BIGINT_TYPE_NAME, "namespace=x")
        .build();
    objectStore.addSchemaVersion(schemaVersion1_2);

    SchemaVersion schemaVersion2_1 = new SchemaVersionBuilder()
        .versionOf(schema2)
        .setVersion(1)
        .addCol("ALPHA", ColumnType.SMALLINT_TYPE_NAME)
        .addCol("delta", ColumnType.DOUBLE_TYPE_NAME)
        .build();
    objectStore.addSchemaVersion(schemaVersion2_1);

    SchemaVersion schemaVersion2_2 = new SchemaVersionBuilder()
        .versionOf(schema2)
        .setVersion(2)
        .addCol("ALPHA", ColumnType.SMALLINT_TYPE_NAME)
        .addCol("delta", ColumnType.DOUBLE_TYPE_NAME)
        .addCol("epsilon", ColumnType.STRING_TYPE_NAME, "namespace=x")
        .build();
    objectStore.addSchemaVersion(schemaVersion2_2);

    // Query that should return nothing
    List<SchemaVersion> results = objectStore.getSchemaVersionsByColumns("x", "y", "z");
    Assert.assertEquals(0, results.size());

    // Query that should fetch one column
    results = objectStore.getSchemaVersionsByColumns("gamma", null, null);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(schemaName1, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(0).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(0).getSchema().getCatName());
    Assert.assertEquals(2, results.get(0).getVersion());

    // fetch 2 in same schema
    results = objectStore.getSchemaVersionsByColumns("beta", null, null);
    Assert.assertEquals(2, results.size());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(0).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(0).getSchema().getCatName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName1, results.get(1).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(1).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(1).getSchema().getCatName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // fetch across schemas
    results = objectStore.getSchemaVersionsByColumns("alpha", null, null);
    Assert.assertEquals(4, results.size());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(0).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(0).getSchema().getCatName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName1, results.get(1).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(1).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(1).getSchema().getCatName());
    Assert.assertEquals(2, results.get(1).getVersion());
    Assert.assertEquals(schemaName2, results.get(2).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(2).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(2).getSchema().getCatName());
    Assert.assertEquals(1, results.get(2).getVersion());
    Assert.assertEquals(schemaName2, results.get(3).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(3).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(3).getSchema().getCatName());
    Assert.assertEquals(2, results.get(3).getVersion());

    // fetch by namespace
    results = objectStore.getSchemaVersionsByColumns(null, "namespace=x", null);
    Assert.assertEquals(2, results.size());
    Collections.sort(results);
    Assert.assertEquals(schemaName1, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(0).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(0).getSchema().getCatName());
    Assert.assertEquals(2, results.get(0).getVersion());
    Assert.assertEquals(schemaName2, results.get(1).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(1).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(1).getSchema().getCatName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // fetch by name and type
    results = objectStore.getSchemaVersionsByColumns("alpha", null, ColumnType.SMALLINT_TYPE_NAME);
    Assert.assertEquals(2, results.size());
    Collections.sort(results);
    Assert.assertEquals(schemaName2, results.get(0).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(0).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(0).getSchema().getCatName());
    Assert.assertEquals(1, results.get(0).getVersion());
    Assert.assertEquals(schemaName2, results.get(1).getSchema().getSchemaName());
    Assert.assertEquals(db.getName(), results.get(1).getSchema().getDbName());
    Assert.assertEquals(db.getCatalogName(), results.get(1).getSchema().getCatName());
    Assert.assertEquals(2, results.get(1).getVersion());

    // Make sure matching name but wrong type doesn't return
    results = objectStore.getSchemaVersionsByColumns("alpha", null, ColumnType.STRING_TYPE_NAME); Assert.assertEquals(0, results.size());
  }

  @Test(expected = MetaException.class)
  public void schemaVersionQueryNoNameOrNamespace() throws MetaException {
    objectStore.getSchemaVersionsByColumns(null, null, ColumnType.STRING_TYPE_NAME);
  }

  private static int dbNum = 1;
  private static Random rand = new Random();
  private Database createUniqueDatabaseForTest() throws MetaException, InvalidObjectException {
    String catName;
    if (rand.nextDouble() < 0.5) {
      catName = "unique_cat_for_test_" + dbNum++;
      objectStore.createCatalog(new CatalogBuilder()
          .setName(catName)
          .setLocation("there")
          .build());
    } else {
      catName = DEFAULT_CATALOG_NAME;
    }
    String dbName = "uniquedbfortest" + dbNum++;
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .setLocation("somewhere")
        .setDescription("descriptive")
        .build(conf);
    objectStore.createDatabase(db);
    return db;
  }
}
