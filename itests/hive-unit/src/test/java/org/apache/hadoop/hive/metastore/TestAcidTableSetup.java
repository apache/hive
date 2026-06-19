/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.thrift.TException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestAcidTableSetup {
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetaStore.class);
  protected static HiveMetaStoreClient client;
  protected static Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();

    MetastoreConf.setClass(conf, ConfVars.EXPRESSION_PROXY_CLASS,
        DefaultPartitionExpressionProxy.class, PartitionExpressionProxy.class);
    client = new HiveMetaStoreClient(conf);
    TestTxnDbUtil.prepDb(conf);
  }

  @Test
  public void testTransactionalValidation() throws Throwable {
    String dbName = "acidDb";
    silentDropDatabase(dbName);
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
    String tblName = "acidTable";
    Map<String, String> fields = new HashMap<>();
    fields.put("name", ColumnType.STRING_TYPE_NAME);
    fields.put("income", ColumnType.INT_TYPE_NAME);

    Type type = createType("Person1", fields);

    Map<String, String> params = new HashMap<>();
    params.put("transactional", "");

    /// CREATE TABLE scenarios

    // Fail - No "transactional" property is specified
    try {
      Table t = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .setTableParams(params)
          .setCols(type.getFields())
          .build(conf);
      client.createTable(t);
      fail("Expected exception");
    } catch (MetaException e) {
      assertEquals("'transactional' property of TBLPROPERTIES may only have value 'true': aciddb.acidtable",
          e.getMessage());
    }

    // Fail - "transactional" property is set to an invalid value
    try {
      params.clear();
      params.put("transactional", "foobar");
      Table t = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .setTableParams(params)
          .setCols(type.getFields())
          .build(conf);
      client.createTable(t);
      fail("Expected exception");
    } catch (MetaException e) {
      assertEquals("'transactional' property of TBLPROPERTIES may only have value 'true': aciddb.acidtable",
          e.getMessage());
    }

    // Fail - "transactional" is set to true, but the table is not bucketed
    try {
      params.clear();
      params.put("transactional", "true");
      Table t = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .setTableParams(params)
          .setCols(type.getFields())
          .build(conf);
      client.createTable(t);
      fail("Expected exception");
    } catch (MetaException e) {
      assertEquals("The table must be stored using an ACID compliant format (such as ORC): aciddb.acidtable",
          e.getMessage());
    }

    List<String> bucketCols = new ArrayList<>();
    bucketCols.add("income");
    // Fail - "transactional" is set to true, and the table is bucketed, but doesn't use ORC
    try {
      params.clear();
      params.put("transactional", "true");
      Table t = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .setTableParams(params)
          .setCols(type.getFields())
          .setBucketCols(bucketCols)
          .build(conf);
      client.createTable(t);
      fail("Expected exception");
    } catch (MetaException e) {
      assertEquals("The table must be stored using an ACID compliant format (such as ORC): aciddb.acidtable",
          e.getMessage());
    }

    // Succeed - "transactional" is set to true, and the table is bucketed, and uses ORC
    params.clear();
    params.put("transactional", "true");
    Table t = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .setTableParams(params)
        .setCols(type.getFields())
        .setBucketCols(bucketCols)
        .setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
        .setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
        .build(conf);
    client.createTable(t);
    assertTrue("CREATE TABLE should succeed",
        "true".equals(t.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL)));

    /// ALTER TABLE scenarios

    // Fail - trying to set "transactional" to "false" is not allowed
    try {
      params.clear();
      params.put("transactional", "false");
      t = new Table();
      t.setParameters(params);
      t.setDbName(dbName);
      t.setTableName(tblName);
      client.alter_table(dbName, tblName, t);
      fail("Expected exception");
    } catch (MetaException e) {
      assertEquals("TBLPROPERTIES with 'transactional'='true' cannot be unset: acidDb.acidTable", e.getMessage());
    }

    // Fail - trying to set "transactional" to "true" but doesn't satisfy bucketing and Input/OutputFormat requirement
    try {
      tblName += "1";
      params.clear();
      t = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .setCols(type.getFields())
          .setInputFormat("org.apache.hadoop.mapred.FileInputFormat")
          .build(conf);
      client.createTable(t);
      params.put("transactional", "true");
      t.setParameters(params);
      client.alter_table(dbName, tblName, t);
      fail("Expected exception");
    } catch (MetaException e) {
      assertEquals("The table must be stored using an ACID compliant format (such as ORC): aciddb.acidtable1",
          e.getMessage());
    }

    // Succeed - trying to set "transactional" to "true", and satisfies bucketing and Input/OutputFormat requirement
    tblName += "2";
    params.clear();
    t = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .setCols(type.getFields())
        .setNumBuckets(1)
        .setBucketCols(bucketCols)
        .setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
        .setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
        .build(conf);
    client.createTable(t);
    params.put("transactional", "true");
    t.setParameters(params);
    client.alter_table(dbName, tblName, t);
    assertTrue("ALTER TABLE should succeed",
        "true".equals(t.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL)));
  }

  private static void silentDropDatabase(String dbName) throws TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException|InvalidOperationException|MetaException e) {
      // NOP
    }
  }

  private Type createType(String typeName, Map<String, String> fields) throws Throwable {
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(fields.size()));
    for(String fieldName : fields.keySet()) {
      typ1.getFields().add(
          new FieldSchema(fieldName, fields.get(fieldName), ""));
    }
    client.createType(typ1);
    return typ1;
  }
}

