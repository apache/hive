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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.zookeeper.Watcher;


/**
 * HBaseQTestUtil defines HBase-specific test fixtures.
 */
public class HBaseQTestUtil extends QTestUtil {

  private String tmpdir;
  
  private MiniHBaseCluster hbase = null;
  private MiniZooKeeperCluster zooKeeperCluster;
  private static final int NUM_REGIONSERVERS = 1;
  
  public HBaseQTestUtil(
    String outDir, String logDir, boolean miniMr) throws Exception {

    super(outDir, logDir, miniMr);
  }
  
  protected void preTestUtilInit() throws Exception {
    // Setup the hbase Cluster
    boolean success = false;
    try {
      conf.set("hbase.master", "local");
      tmpdir =  System.getProperty("user.dir")+"/../build/ql/tmp";
      conf.set("hbase.rootdir", "file://" + tmpdir + "/hbase");
      zooKeeperCluster = new MiniZooKeeperCluster();
      int clientPort = zooKeeperCluster.startup(
        new File(tmpdir, "zookeeper"));
      conf.set("hbase.zookeeper.property.clientPort",
        Integer.toString(clientPort));
      HBaseConfiguration hbaseConf = new HBaseConfiguration(conf);
      hbase = new MiniHBaseCluster(hbaseConf, NUM_REGIONSERVERS);
      conf.set("hbase.master", hbase.getHMasterAddress().toString());
      // opening the META table ensures that cluster is running
      new HTable(new HBaseConfiguration(conf), HConstants.META_TABLE_NAME);
      success = true;
    } finally {
      if (!success) {
        if (hbase != null) {
          hbase.shutdown();
        }
        if (zooKeeperCluster != null) {
          zooKeeperCluster.shutdown();
        }
      }
    }
    
    String auxJars = conf.getAuxJars();
    auxJars = ((auxJars == null) ? "" : (auxJars + ",")) + "file://"
      + new JobConf(conf, HBaseConfiguration.class).getJar();
    auxJars += ",file://" + new JobConf(conf, HBaseSerDe.class).getJar();
    auxJars += ",file://" + new JobConf(conf, Watcher.class).getJar();
    conf.setAuxJars(auxJars);
  }
  
  public void shutdown() throws Exception {
    if (hbase != null) {
      HConnectionManager.deleteConnectionInfo(
        new HBaseConfiguration(conf), true);
      hbase.shutdown();
      hbase = null;
    }
    if (zooKeeperCluster != null) {
      zooKeeperCluster.shutdown();
      zooKeeperCluster = null;
    }
    
    super.shutdown();
  }

}
