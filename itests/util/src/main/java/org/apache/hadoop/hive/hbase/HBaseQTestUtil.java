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
package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.QTestUtil;

/**
 * HBaseQTestUtil initializes HBase-specific test fixtures.
 */
public class HBaseQTestUtil extends QTestUtil {

  /** Name of the HBase table, in both Hive and HBase. */
  public static String HBASE_SRC_NAME = "src_hbase";

  /** Name of the table snapshot. */
  public static String HBASE_SRC_SNAPSHOT_NAME = "src_hbase_snapshot";

  /** A handle to this harness's cluster */
  private final Connection conn;

  private HBaseTestSetup hbaseSetup = null;

  public HBaseQTestUtil(
    String outDir, String logDir, MiniClusterType miniMr, HBaseTestSetup setup,
    String initScript, String cleanupScript)
    throws Exception {

    super(outDir, logDir, miniMr, null, "0.20", initScript, cleanupScript, false);
    hbaseSetup = setup;
    hbaseSetup.preTest(conf);
    this.conn = setup.getConnection();
    super.init();
  }

  @Override
  public void init() throws Exception {
    // defer
  }

  @Override
  protected void initConfFromSetup() throws Exception {
    super.initConfFromSetup();
    hbaseSetup.preTest(conf);
  }

  @Override
  public void createSources(String tname) throws Exception {
    super.createSources(tname);

    conf.setBoolean("hive.test.init.phase", true);

    // create and load the input data into the hbase table
    runCreateTableCmd(
      "CREATE TABLE " + HBASE_SRC_NAME + "(key INT, value STRING)"
        + "  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
        + "  WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf:val')"
        + "  TBLPROPERTIES ('hbase.table.name' = '" + HBASE_SRC_NAME + "')"
    );
    runCmd("INSERT OVERWRITE TABLE " + HBASE_SRC_NAME + " SELECT * FROM src");

    // create a snapshot
    Admin admin = null;
    try {
      admin = conn.getAdmin();
      admin.snapshot(HBASE_SRC_SNAPSHOT_NAME, TableName.valueOf(HBASE_SRC_NAME));
    } finally {
      if (admin != null) admin.close();
    }

    conf.setBoolean("hive.test.init.phase", false);
  }

  @Override
  public void cleanUp(String tname) throws Exception {
    super.cleanUp(tname);

    // drop in case leftover from unsuccessful run
    db.dropTable(Warehouse.DEFAULT_DATABASE_NAME, HBASE_SRC_NAME);

    Admin admin = null;
    try {
      admin = conn.getAdmin();
      admin.deleteSnapshots(HBASE_SRC_SNAPSHOT_NAME);
    } finally {
      if (admin != null) admin.close();
    }
  }

  @Override
  public void clearTestSideEffects() throws Exception {
    super.clearTestSideEffects();
    hbaseSetup.preTest(conf);
  }
}
