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

package org.apache.hadoop.hive.hbase;

import java.io.File;

import junit.extensions.TestSetup;
import junit.framework.Test;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.zookeeper.Watcher;

/**
 * HBaseTestSetup defines HBase-specific test fixtures which are
 * reused across testcases.
 */
public class HBaseTestSetup extends TestSetup
{
  private MiniHBaseCluster hbaseCluster;
  private MiniZooKeeperCluster zooKeeperCluster;
  private int zooKeeperPort;
  private String hbaseRoot;

  private static final int NUM_REGIONSERVERS = 1;

  public HBaseTestSetup(Test test) {
    super(test);
  }
  
  void preTest(HiveConf conf) throws Exception {
    if (hbaseCluster == null) {
      // We set up fixtures on demand for the first testcase, and leave
      // them allocated for reuse across all others.  Then tearDown
      // will get called once at the very end after all testcases have
      // run, giving us a guaranteed opportunity to shut them down.
      setUpFixtures(conf);
    }
    conf.set("hbase.rootdir", hbaseRoot);
    conf.set("hbase.master", hbaseCluster.getHMasterAddress().toString());
    conf.set("hbase.zookeeper.property.clientPort",
      Integer.toString(zooKeeperPort));
    String auxJars = conf.getAuxJars();
    auxJars = ((auxJars == null) ? "" : (auxJars + ",")) + "file://"
      + new JobConf(conf, HBaseConfiguration.class).getJar();
    auxJars += ",file://" + new JobConf(conf, HBaseSerDe.class).getJar();
    auxJars += ",file://" + new JobConf(conf, Watcher.class).getJar();
    conf.setAuxJars(auxJars);
  }

  private void setUpFixtures(HiveConf conf) throws Exception {
    conf.set("hbase.master", "local");
    String tmpdir =  System.getProperty("user.dir")+"/../build/ql/tmp";
    hbaseRoot = "file://" + tmpdir + "/hbase";
    conf.set("hbase.rootdir", hbaseRoot);
    zooKeeperCluster = new MiniZooKeeperCluster();
    zooKeeperPort = zooKeeperCluster.startup(
      new File(tmpdir, "zookeeper"));
    conf.set("hbase.zookeeper.property.clientPort",
      Integer.toString(zooKeeperPort));
    HBaseConfiguration hbaseConf = new HBaseConfiguration(conf);
    hbaseCluster = new MiniHBaseCluster(hbaseConf, NUM_REGIONSERVERS);
    conf.set("hbase.master", hbaseCluster.getHMasterAddress().toString());
    // opening the META table ensures that cluster is running
    new HTable(new HBaseConfiguration(conf), HConstants.META_TABLE_NAME);
  }

  @Override
  protected void tearDown() throws Exception {
    if (hbaseCluster != null) {
      HConnectionManager.deleteAllConnections(true);
      hbaseCluster.shutdown();
      hbaseCluster = null;
    }
    if (zooKeeperCluster != null) {
      zooKeeperCluster.shutdown();
      zooKeeperCluster = null;
    }
  }
}
