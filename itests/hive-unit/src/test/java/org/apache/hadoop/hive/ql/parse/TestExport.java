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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestExport {

  protected static final Logger LOG = LoggerFactory.getLogger(TestExport.class);
  private static WarehouseInstance hiveWarehouse;

  @Rule
  public final TestName testName = new TestName();
  private String dbName;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.client.use.datanode.hostname", "true");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    hiveWarehouse = new WarehouseInstance(LOG, miniDFSCluster, false);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    hiveWarehouse.close();
  }

  @Before
  public void setup() throws Throwable {
    dbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    hiveWarehouse.run("create database " + dbName);
  }

  @Test
  public void shouldExportImportATemporaryTable() throws Throwable {
    String path = "hdfs:///tmp/" + dbName + "/";
    String exportPath = "'" + path + "'";
    String importDataPath = path + "/data";
    hiveWarehouse
        .run("use " + dbName)
        .run("create temporary table t1 (i int)")
        .run("insert into table t1 values (1),(2)")
        .run("export table t1 to " + exportPath)
        .run("create temporary table t2 like t1")
        .run("load data inpath '" + importDataPath + "' overwrite into table t2")
        .run("select * from t2")
        .verifyResults(new String[] { "1", "2" });
  }
}
