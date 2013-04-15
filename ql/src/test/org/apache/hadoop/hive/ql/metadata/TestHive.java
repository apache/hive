/**
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

package org.apache.hadoop.hive.ql.metadata;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.index.HiveIndex;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer;
import org.apache.hadoop.hive.serde2.thrift.test.Complex;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * TestHive.
 *
 */
public class TestHive extends TestCase {
  protected Hive hm;
  protected HiveConf hiveConf;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());
    try {
      hm = Hive.get(hiveConf);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err
          .println("Unable to initialize Hive Metastore using configuration: \n "
          + hiveConf);
      throw e;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      super.tearDown();
      Hive.closeCurrent();
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err
          .println("Unable to close Hive Metastore using configruation: \n "
          + hiveConf);
      throw e;
    }
  }

  public void testTable() throws Throwable {
    try {
      // create a simple table and test create, drop, get
      String tableName = "table_for_testtable";
      try {
        hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e1) {
        e1.printStackTrace();
        assertTrue("Unable to drop table", false);
      }
      Table tbl = new Table(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      List<FieldSchema> fields = tbl.getCols();

      fields.add(new FieldSchema("col1", serdeConstants.INT_TYPE_NAME, "int -- first column"));
      fields.add(new FieldSchema("col2", serdeConstants.STRING_TYPE_NAME, "string -- second column"));
      fields.add(new FieldSchema("col3", serdeConstants.DOUBLE_TYPE_NAME, "double -- thrift column"));
      tbl.setFields(fields);

      tbl.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
      tbl.setInputFormatClass(SequenceFileInputFormat.class);

      tbl.setProperty("comment", "this is a test table created as part junit tests");

      List<String> bucketCols = tbl.getBucketCols();
      bucketCols.add("col1");
      try {
        tbl.setBucketCols(bucketCols);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to set bucket column for table: " + tableName, false);
      }

      List<FieldSchema> partCols = new ArrayList<FieldSchema>();
      partCols
          .add(new FieldSchema(
          "ds",
          serdeConstants.STRING_TYPE_NAME,
          "partition column, date but in string format as date type is not yet supported in QL"));
      tbl.setPartCols(partCols);

      tbl.setNumBuckets((short) 512);
      tbl.setOwner("pchakka");
      tbl.setRetention(10);

      // set output format parameters (these are not supported by QL but only
      // for demo purposes)
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, "1");
      tbl.setSerdeParam(serdeConstants.LINE_DELIM, "\n");
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, "3");
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, "2");

      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, "1");
      tbl.setSerializationLib(LazySimpleSerDe.class.getName());
      tbl.setStoredAsSubDirectories(false);

      // create table
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to create table: " + tableName, false);
      }

      // get table
      validateTable(tbl, tableName);

      try {
        hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, true,
            false);
        Table ft2 = hm.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,
            tableName, false);
        assertNull("Unable to drop table ", ft2);
      } catch (HiveException e) {
        assertTrue("Unable to drop table: " + tableName, false);
      }
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTable failed");
      throw e;
    }
  }

  /**
   * Tests create and fetch of a thrift based table.
   *
   * @throws Throwable
   */
  public void testThriftTable() throws Throwable {
    String tableName = "table_for_test_thrifttable";
    try {
      try {
        hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e1) {
        System.err.println(StringUtils.stringifyException(e1));
        assertTrue("Unable to drop table", false);
      }
      Table tbl = new Table(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      tbl.setInputFormatClass(SequenceFileInputFormat.class.getName());
      tbl.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
      tbl.setSerializationLib(ThriftDeserializer.class.getName());
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_CLASS, Complex.class.getName());
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, TBinaryProtocol.class
          .getName());
      tbl.setStoredAsSubDirectories(false);
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create table: " + tableName, false);
      }
      // get table
      validateTable(tbl, tableName);
      hm.dropTable(DEFAULT_DATABASE_NAME, tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testThriftTable() failed");
      throw e;
    }
  }

  /**
   * Gets a table from the metastore and compares it to the original Table
   *
   * @param tbl
   * @param tableName
   * @throws MetaException
   */
  private void validateTable(Table tbl, String tableName) throws MetaException {
    Warehouse wh = new Warehouse(hiveConf);
    Table ft = null;
    try {
      ft = hm.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      assertNotNull("Unable to fetch table", ft);
      ft.checkValidity();
      assertEquals("Table names didn't match for table: " + tableName, tbl
          .getTableName(), ft.getTableName());
      assertEquals("Table owners didn't match for table: " + tableName, tbl
          .getOwner(), ft.getOwner());
      assertEquals("Table retention didn't match for table: " + tableName,
          tbl.getRetention(), ft.getRetention());
      assertEquals("Data location is not set correctly",
          wh.getTablePath(hm.getDatabase(DEFAULT_DATABASE_NAME), tableName).toString(),
          ft.getDataLocation().toString());
      // now that URI and times are set correctly, set the original table's uri and times
      // and then compare the two tables
      tbl.setDataLocation(ft.getDataLocation());
      tbl.setCreateTime(ft.getTTable().getCreateTime());
      tbl.getParameters().put(hive_metastoreConstants.DDL_TIME,
          ft.getParameters().get(hive_metastoreConstants.DDL_TIME));
      assertTrue("Tables  doesn't match: " + tableName, ft.getTTable()
          .equals(tbl.getTTable()));
      assertEquals("SerializationLib is not set correctly", tbl
          .getSerializationLib(), ft.getSerializationLib());
      assertEquals("Serde is not set correctly", tbl.getDeserializer()
          .getClass().getName(), ft.getDeserializer().getClass().getName());
    } catch (HiveException e) {
      System.err.println(StringUtils.stringifyException(e));
      assertTrue("Unable to fetch table correctly: " + tableName, false);
    }
  }

  private static Table createTestTable(String dbName, String tableName) throws HiveException {
    Table tbl = new Table(dbName, tableName);
    tbl.setInputFormatClass(SequenceFileInputFormat.class.getName());
    tbl.setOutputFormatClass(SequenceFileOutputFormat.class.getName());
    tbl.setSerializationLib(ThriftDeserializer.class.getName());
    tbl.setSerdeParam(serdeConstants.SERIALIZATION_CLASS, Complex.class.getName());
    tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, TBinaryProtocol.class
        .getName());
    return tbl;
  }

  /**
   * Test basic Hive class interaction, that:
   * - We can have different Hive objects throughout the lifetime of this thread.
   */
  public void testHiveCloseCurrent() throws Throwable {
    Hive hive1 = Hive.get();
    Hive.closeCurrent();
    Hive hive2 = Hive.get();
    Hive.closeCurrent();
    assertTrue(hive1 != hive2);
  }

  public void testGetAndDropTables() throws Throwable {
    try {
      String dbName = "db_for_testgettables";
      String table1Name = "table1";
      hm.dropDatabase(dbName, true, true);

      Database db = new Database();
      db.setName(dbName);
      hm.createDatabase(db);

      List<String> ts = new ArrayList<String>(2);
      ts.add(table1Name);
      ts.add("table2");
      Table tbl1 = createTestTable(dbName, ts.get(0));
      hm.createTable(tbl1);

      Table tbl2 = createTestTable(dbName, ts.get(1));
      hm.createTable(tbl2);

      List<String> fts = hm.getTablesForDb(dbName, ".*");
      assertEquals(ts, fts);
      assertEquals(2, fts.size());

      fts = hm.getTablesForDb(dbName, ".*1");
      assertEquals(1, fts.size());
      assertEquals(ts.get(0), fts.get(0));

      // also test getting a table from a specific db
      Table table1 = hm.getTable(dbName, table1Name);
      assertNotNull(table1);
      assertEquals(table1Name, table1.getTableName());

      FileSystem fs = table1.getPath().getFileSystem(hiveConf);
      assertTrue(fs.exists(table1.getPath()));
      // and test dropping this specific table
      hm.dropTable(dbName, table1Name);
      assertFalse(fs.exists(table1.getPath()));

      // Drop all tables
      for (String tableName : hm.getAllTables(dbName)) {
        hm.dropTable(dbName, tableName);
      }
      hm.dropDatabase(dbName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testGetTables() failed");
      throw e;
    }
  }

  public void testPartition() throws Throwable {
    try {
      String tableName = "table_for_testpartition";
      try {
        hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to drop table: " + tableName, false);
      }
      LinkedList<String> cols = new LinkedList<String>();
      cols.add("key");
      cols.add("value");

      LinkedList<String> part_cols = new LinkedList<String>();
      part_cols.add("ds");
      part_cols.add("hr");
      try {
        hm.createTable(tableName, cols, part_cols, TextInputFormat.class,
            HiveIgnoreKeyTextOutputFormat.class);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create table: " + tableName, false);
      }
      Table tbl = null;
      try {
        tbl = hm.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to fetch table: " + tableName, false);
      }
      HashMap<String, String> part_spec = new HashMap<String, String>();
      part_spec.clear();
      part_spec.put("ds", "2008-04-08");
      part_spec.put("hr", "12");
      try {
        hm.createPartition(tbl, part_spec);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to create parition for table: " + tableName, false);
      }
      hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testPartition() failed");
      throw e;
    }
  }

  /**
   * Tests creating a simple index on a simple table.
   *
   * @throws Throwable
   */
  public void testIndex() throws Throwable {
    try{
      // create a simple table
      String tableName = "table_for_testindex";
      try {
        hm.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to drop table", false);
      }

      Table tbl = new Table(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      List<FieldSchema> fields = tbl.getCols();

      fields.add(new FieldSchema("col1", serdeConstants.INT_TYPE_NAME, "int -- first column"));
      fields.add(new FieldSchema("col2", serdeConstants.STRING_TYPE_NAME,
          "string -- second column"));
      fields.add(new FieldSchema("col3", serdeConstants.DOUBLE_TYPE_NAME,
          "double -- thrift column"));
      tbl.setFields(fields);

      tbl.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
      tbl.setInputFormatClass(SequenceFileInputFormat.class);

      // create table
      try {
        hm.createTable(tbl);
      } catch (HiveException e) {
        e.printStackTrace();
        assertTrue("Unable to create table: " + tableName, false);
      }

      // Create a simple index
      String indexName = "index_on_table_for_testindex";
      String indexHandlerClass = HiveIndex.IndexType.COMPACT_SUMMARY_TABLE.getHandlerClsName();
      List<String> indexedCols = new ArrayList<String>();
      indexedCols.add("col1");
      String indexTableName = "index_on_table_for_testindex_table";
      boolean deferredRebuild = true;
      String inputFormat = SequenceFileInputFormat.class.getName();
      String outputFormat = SequenceFileOutputFormat.class.getName();
      String serde = null;
      String storageHandler = null;
      String location = null;
      String collItemDelim = null;
      String fieldDelim = null;
      String fieldEscape = null;
      String lineDelim = null;
      String mapKeyDelim = null;
      String indexComment = null;
      Map<String, String> indexProps = null;
      Map<String, String> tableProps = null;
      Map<String, String> serdeProps = new HashMap<String, String>();
      hm.createIndex(tableName, indexName, indexHandlerClass, indexedCols, indexTableName,
          deferredRebuild, inputFormat, outputFormat, serde, storageHandler, location,
          indexProps, tableProps, serdeProps, collItemDelim, fieldDelim, fieldEscape, lineDelim,
          mapKeyDelim, indexComment);

      // Retrieve and validate the index
      Index index = null;
      try {
        index = hm.getIndex(tableName, indexName);
        assertNotNull("Unable to fetch index", index);
        index.validate();
        assertEquals("Index names don't match for index: " + indexName, indexName,
            index.getIndexName());
        assertEquals("Table names don't match for index: " + indexName, tableName,
            index.getOrigTableName());
        assertEquals("Index table names didn't match for index: " + indexName, indexTableName,
            index.getIndexTableName());
        assertEquals("Index handler classes didn't match for index: " + indexName,
            indexHandlerClass, index.getIndexHandlerClass());
        assertEquals("Deferred rebuild didn't match for index: " + indexName, deferredRebuild,
            index.isDeferredRebuild());

      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to fetch index correctly: " + indexName, false);
      }

      // Drop index
      try {
        hm.dropIndex(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, indexName, true);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to drop index: " + indexName, false);
      }

      boolean dropIndexException = false;
      try {
        hm.getIndex(tableName, indexName);
      } catch (HiveException e) {
        // Expected since it was just dropped
        dropIndexException = true;
      }

      assertTrue("Unable to drop index: " + indexName, dropIndexException);

      // Drop table
      try {
        hm.dropTable(tableName);
        Table droppedTable = hm.getTable(tableName, false);
        assertNull("Unable to drop table " + tableName, droppedTable);
      } catch (HiveException e) {
        System.err.println(StringUtils.stringifyException(e));
        assertTrue("Unable to drop table: " + tableName, false);
      }
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testIndex failed");
      throw e;
    }
  }

  public void testHiveRefreshDatabase() throws Throwable{
    String testDatabaseName = "test_database";
    Database testDatabase = new Database();
    testDatabase.setName(testDatabaseName);
    hm.createDatabase(testDatabase, true);
    hm.setCurrentDatabase(testDatabaseName);
    hm = Hive.get(hiveConf, true); //refresh Hive instance
    assertEquals(testDatabaseName, hm.getCurrentDatabase());
  }
}
