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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.cli.HCatDriver;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.hbase.SkeletonHBaseTest;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestZNodeSetUp extends SkeletonHBaseTest {

  private static HiveConf hcatConf;
  private static HCatDriver hcatDriver;

  @BeforeClass
  public static void setup() throws Throwable {
    setupSkeletonHBaseTest();
  }

  public void Initialize() throws Exception {

    hcatConf = getHiveConf();
    hcatConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
      HCatSemanticAnalyzer.class.getName());
    URI fsuri = getFileSystem().getUri();
    Path whPath = new Path(fsuri.getScheme(), fsuri.getAuthority(),
      getTestDir());
    hcatConf.set(HiveConf.ConfVars.HADOOPFS.varname, fsuri.toString());
    hcatConf.set(ConfVars.METASTOREWAREHOUSE.varname, whPath.toString());

    //Add hbase properties

    for (Map.Entry<String, String> el : getHbaseConf()) {
      if (el.getKey().startsWith("hbase.")) {
        hcatConf.set(el.getKey(), el.getValue());
      }
    }
    HBaseConfiguration.merge(hcatConf,
      RevisionManagerConfiguration.create());
    hcatConf.set(RMConstants.ZOOKEEPER_DATADIR, "/rm_base");
    SessionState.start(new CliSessionState(hcatConf));
    hcatDriver = new HCatDriver();

  }

  @Test
  public void testBasicZNodeCreation() throws Exception {

    Initialize();
    int port = getHbaseConf().getInt("hbase.zookeeper.property.clientPort", 2181);
    String servers = getHbaseConf().get("hbase.zookeeper.quorum");
    String[] splits = servers.split(",");
    StringBuffer sb = new StringBuffer();
    for (String split : splits) {
      sb.append(split);
      sb.append(':');
      sb.append(port);
    }

    hcatDriver.run("drop table test_table");
    CommandProcessorResponse response = hcatDriver
      .run("create table test_table(key int, value string) STORED BY " +
        "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
        + "TBLPROPERTIES ('hbase.columns.mapping'=':key,cf1:val')");

    assertEquals(0, response.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists("test_table");
    assertTrue(doesTableExist);


    ZKUtil zkutil = new ZKUtil(sb.toString(), "/rm_base");
    ZooKeeper zk = zkutil.getSession();
    String tablePath = PathUtil.getTxnDataPath("/rm_base", "test_table");
    Stat tempTwo = zk.exists(tablePath, false);
    assertTrue(tempTwo != null);

    String cfPath = PathUtil.getTxnDataPath("/rm_base", "test_table") + "/cf1";
    Stat tempThree = zk.exists(cfPath, false);
    assertTrue(tempThree != null);

    hcatDriver.run("drop table test_table");

    System.out.println("Table path : " + tablePath);
    Stat tempFour = zk.exists(tablePath, false);
    assertTrue(tempFour == null);

  }

}
