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

package org.apache.hadoop.hive.ql.parse;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test ParseUtils to see if a staging dir is created.
 */
public class TestParseUtilsStagingDir {
  protected static final Logger LOG = LoggerFactory.getLogger(TestParseUtilsStagingDir.class.getName());

  private static String jksFile = System.getProperty("java.io.tmpdir") + "/test.jks";

  private static WarehouseInstance primary;
  private static HiveConf conf;
  private static MiniDFSCluster miniDFSCluster;

  private static final String TABLE = "tbl";

  @BeforeClass
  public static void beforeClassSetup() throws Exception {
    System.setProperty("jceks.key.serialFilter", "java.lang.Enum;java.security.KeyRep;" +
        "java.security.KeyRep$Type;javax.crypto.spec.SecretKeySpec;" +
        "org.apache.hadoop.crypto.key.JavaKeyStoreProvider$KeyMetadata;!*");
    conf = new HiveConfForTest(TestParseUtilsStagingDir.class);
    conf.set("hadoop.security.key.provider.path", "jceks://file" + jksFile);

    miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();

    DFSTestUtil.createKey("test_key", miniDFSCluster, conf);
    primary = new WarehouseInstance(LOG, miniDFSCluster, new HashMap<String, String>(),
        "test_key");
    SessionState.start((HiveConf) conf);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    SessionState.get().close();
    primary.close();
    FileUtils.deleteQuietly(new File(jksFile));
  }

  /**
   * Test to make sure staging directory is not created when the
   * isLoadingMaterializedView flag is true and the file system
   * is encrypted.
   */
  @Test
  public void testGetStagingDirEncryptedWithMV() throws Exception {
    Table table = createTable(TABLE, primary.warehouseRoot);
    QB qb = createQB(table);
    Context ctx = new Context(conf);
    ctx.setIsLoadingMaterializedView(true);
    Path path = SemanticAnalyzer.getStagingDirectoryPathname(qb, conf, ctx);
    Assert.assertFalse(miniDFSCluster.getFileSystem().exists(path.getParent()));
  }

  /**
   * Test to make sure staging directory is created when with no flags set when
   * the file system is encrypted.
   *
   * Note: This might not be the correct behavior, but this was the behavior
   * HIVE-28173 was fixed. It is too risky to change behavior for
   * the default path.
   */
  @Test
  public void testGetStagingDirEncrypted() throws Exception {
    Table table = createTable(TABLE, primary.warehouseRoot);
    QB qb = createQB(table);
    Context ctx = new Context(conf);
    Path path = SemanticAnalyzer.getStagingDirectoryPathname(qb, conf, ctx);
    Assert.assertTrue(miniDFSCluster.getFileSystem().exists(path.getParent()));
  }

  /**
   * Test that no staging directory is created for default behavior where the
   * directory location is on the local file system.
   */
  @Test
  public void testGetStagingDir() throws Exception {
    Context ctx = new Context(conf);
    QB qb = new QB("", "", false);
    Path path = SemanticAnalyzer.getStagingDirectoryPathname(qb, conf, ctx);
    FileSystem fs = FileSystem.getLocal(conf);
    Assert.assertFalse(fs.exists(path.getParent()));
  }

  private static QB createQB(Table table) {
    String tableName = table.getTTable().getTableName();
    QB qb = new QB("", "", false);
    qb.setTabAlias(tableName, tableName);
    qb.getMetaData().setSrcForAlias(tableName, table);
    return qb;
  }

  private static Table createTable(String tableName, Path location) {
    Table table = new Table();
    table.setTTable(createTTableObject(tableName));
    table.setDataLocation(location);
    return table;
  }

  private static org.apache.hadoop.hive.metastore.api.Table createTTableObject(String tableName) {
    org.apache.hadoop.hive.metastore.api.Table tTable =
        new org.apache.hadoop.hive.metastore.api.Table();
    tTable.setSd(new StorageDescriptor());
    tTable.setTableName(tableName);
    return tTable;
  }
}
