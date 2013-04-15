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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.thrift.TException;

/**
 * TestHiveMetaStoreChecker.
 *
 */
public class TestHiveMetaStoreChecker extends TestCase {

  private Hive hive;
  private FileSystem fs;
  private HiveMetaStoreChecker checker = null;

  private final String dbName = "dbname";
  private final String tableName = "tablename";

  private final String partDateName = "partdate";
  private final String partCityName = "partcity";

  private List<FieldSchema> partCols;
  private List<Map<String, String>> parts;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hive = Hive.get();
    checker = new HiveMetaStoreChecker(hive);

    partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema(partDateName, serdeConstants.STRING_TYPE_NAME, ""));
    partCols.add(new FieldSchema(partCityName, serdeConstants.STRING_TYPE_NAME, ""));

    parts = new ArrayList<Map<String, String>>();
    Map<String, String> part1 = new HashMap<String, String>();
    part1.put(partDateName, "2008-01-01");
    part1.put(partCityName, "london");
    parts.add(part1);
    Map<String, String> part2 = new HashMap<String, String>();
    part2.put(partDateName, "2008-01-02");
    part2.put(partCityName, "stockholm");
    parts.add(part2);

    // cleanup
    hive.dropTable(dbName, tableName, true, true);
    try {
      hive.dropDatabase(dbName);
    } catch (NoSuchObjectException e) {
      // ignore
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    Hive.closeCurrent();
  }

  public void testTableCheck() throws HiveException, MetaException,
      IOException, TException, AlreadyExistsException {

    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    // we haven't added anything so should return an all ok
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    // check table only, should not exist in ms
    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertEquals(1, result.getTablesNotInMs().size());
    assertEquals(tableName, result.getTablesNotInMs().get(0));
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    Database db = new Database();
    db.setName(dbName);
    hive.createDatabase(db);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    table.setInputFormatClass(TextInputFormat.class);
    table.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);

    hive.createTable(table);
    // now we've got a table, check that it works
    // first check all (1) tables
    result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    // then let's check the one we know about
    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    // remove the table folder
    fs = table.getPath().getFileSystem(hive.getConf());
    fs.delete(table.getPath(), true);

    // now this shouldn't find the path on the fs
    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertEquals(1, result.getTablesNotOnFs().size());
    assertEquals(tableName, result.getTablesNotOnFs().get(0));
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    // put it back and one additional table
    fs.mkdirs(table.getPath());
    Path fakeTable = table.getPath().getParent().suffix(
        Path.SEPARATOR + "faketable");
    fs.mkdirs(fakeTable);

    // find the extra table
    result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    assertEquals(1, result.getTablesNotInMs().size());
    assertEquals(fakeTable.getName(), result.getTablesNotInMs().get(0));
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    // create a new external table
    hive.dropTable(dbName, tableName);
    table.setProperty("EXTERNAL", "TRUE");
    hive.createTable(table);

    // should return all ok
    result = new CheckResult();
    checker.checkMetastore(dbName, null, null, result);
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());
  }

  public void testPartitionsCheck() throws HiveException, MetaException,
      IOException, TException, AlreadyExistsException {

    Database db = new Database();
    db.setName(dbName);
    hive.createDatabase(db);

    Table table = new Table(dbName, tableName);
    table.setDbName(dbName);
    table.setInputFormatClass(TextInputFormat.class);
    table.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    table.setPartCols(partCols);

    hive.createTable(table);
    table = hive.getTable(dbName, tableName);

    for (Map<String, String> partSpec : parts) {
      hive.createPartition(table, partSpec);
    }

    CheckResult result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    // all is well
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    List<Partition> partitions = hive.getPartitions(table);
    assertEquals(2, partitions.size());
    Partition partToRemove = partitions.get(0);
    Path partToRemovePath = new Path(partToRemove.getDataLocation().toString());
    fs = partToRemovePath.getFileSystem(hive.getConf());
    fs.delete(partToRemovePath, true);

    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, null, result);
    // missing one partition on fs
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertEquals(1, result.getPartitionsNotOnFs().size());
    assertEquals(partToRemove.getName(), result.getPartitionsNotOnFs().get(0)
        .getPartitionName());
    assertEquals(partToRemove.getTable().getTableName(), result
        .getPartitionsNotOnFs().get(0).getTableName());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    List<Map<String, String>> partsCopy = new ArrayList<Map<String, String>>();
    partsCopy.add(partitions.get(1).getSpec());
    // check only the partition that exists, all should be well
    result = new CheckResult();
    checker.checkMetastore(dbName, tableName, partsCopy, result);
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotInMs().isEmpty());

    // put the other one back
    fs.mkdirs(partToRemovePath);

    // add a partition dir on fs
    Path fakePart = new Path(table.getDataLocation().toString(),
        "fakepartition=fakevalue");
    fs.mkdirs(fakePart);

    checker.checkMetastore(dbName, tableName, null, result);
    // one extra partition
    assertTrue(result.getTablesNotInMs().isEmpty());
    assertTrue(result.getTablesNotOnFs().isEmpty());
    assertTrue(result.getPartitionsNotOnFs().isEmpty());
    assertEquals(1, result.getPartitionsNotInMs().size());
    assertEquals(fakePart.getName(), result.getPartitionsNotInMs().get(0)
        .getPartitionName());
  }

}
