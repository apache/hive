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

package org.apache.hive.hcatalog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

/**
 * MiniCluster class composed of a number of Hadoop Minicluster implementations
 * and other necessary daemons needed for testing (HBase, Hive MetaStore, Zookeeper, MiniMRCluster)
 */
public class ManyMiniCluster {

  //MR stuff
  private boolean miniMRClusterEnabled;
  private MiniMRCluster mrCluster;
  private int numTaskTrackers;
  private JobConf jobConf;

  //HBase stuff
  private boolean miniHBaseClusterEnabled;
  private MiniHBaseCluster hbaseCluster;
  private String hbaseRoot;
  private Configuration hbaseConf;
  private String hbaseDir;

  //ZK Stuff
  private boolean miniZookeeperClusterEnabled;
  private MiniZooKeeperCluster zookeeperCluster;
  private int zookeeperPort;
  private String zookeeperDir;

  //DFS Stuff
  private MiniDFSCluster dfsCluster;

  //Hive Stuff
  private boolean miniHiveMetastoreEnabled;
  private HiveConf hiveConf;
  private HiveMetaStoreClient hiveMetaStoreClient;

  private final File workDir;
  private boolean started = false;


  /**
   * create a cluster instance using a builder which will expose configurable options
   * @param workDir working directory ManyMiniCluster will use for all of it's *Minicluster instances
   * @return a Builder instance
   */
  public static Builder create(File workDir) {
    return new Builder(workDir);
  }

  private ManyMiniCluster(Builder b) {
    workDir = b.workDir;
    numTaskTrackers = b.numTaskTrackers;
    hiveConf = b.hiveConf;
    jobConf = b.jobConf;
    hbaseConf = b.hbaseConf;
    miniMRClusterEnabled = b.miniMRClusterEnabled;
    miniHBaseClusterEnabled = b.miniHBaseClusterEnabled;
    miniHiveMetastoreEnabled = b.miniHiveMetastoreEnabled;
    miniZookeeperClusterEnabled = b.miniZookeeperClusterEnabled;
  }

  protected synchronized void start() {
    try {
      if (!started) {
        FileUtil.fullyDelete(workDir);
        if (miniMRClusterEnabled) {
          setupMRCluster();
        }
        if (miniZookeeperClusterEnabled || miniHBaseClusterEnabled) {
          miniZookeeperClusterEnabled = true;
          setupZookeeper();
        }
        if (miniHBaseClusterEnabled) {
          setupHBaseCluster();
        }
        if (miniHiveMetastoreEnabled) {
          setUpMetastore();
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to setup cluster", e);
    }
  }

  protected synchronized void stop() {
    if (hbaseCluster != null) {
      HConnectionManager.deleteAllConnections(true);
      try {
        hbaseCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
      hbaseCluster = null;
    }
    if (zookeeperCluster != null) {
      try {
        zookeeperCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
      zookeeperCluster = null;
    }
    if (mrCluster != null) {
      try {
        mrCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
      mrCluster = null;
    }
    if (dfsCluster != null) {
      try {
        dfsCluster.getFileSystem().close();
        dfsCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
      dfsCluster = null;
    }
    try {
      FileSystem.closeAll();
    } catch (IOException e) {
      e.printStackTrace();
    }
    started = false;
  }

  /**
   * @return Configuration of mini HBase cluster
   */
  public Configuration getHBaseConf() {
    return HBaseConfiguration.create(hbaseConf);
  }

  /**
   * @return Configuration of mini MR cluster
   */
  public Configuration getJobConf() {
    return new Configuration(jobConf);
  }

  /**
   * @return Configuration of Hive Metastore, this is a standalone not a daemon
   */
  public HiveConf getHiveConf() {
    return new HiveConf(hiveConf);
  }

  /**
   * @return Filesystem used by MiniMRCluster and MiniHBaseCluster
   */
  public FileSystem getFileSystem() {
    try {
      return FileSystem.get(jobConf);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get FileSystem", e);
    }
  }

  /**
   * @return Metastore client instance
   */
  public HiveMetaStoreClient getHiveMetaStoreClient() {
    return hiveMetaStoreClient;
  }

  private void setupMRCluster() {
    try {
      final int jobTrackerPort = findFreePort();
      final int taskTrackerPort = findFreePort();

      if (jobConf == null)
        jobConf = new JobConf();

      jobConf.setInt("mapred.submit.replication", 1);
      jobConf.set("yarn.scheduler.capacity.root.queues", "default");
      jobConf.set("yarn.scheduler.capacity.root.default.capacity", "100");
      //conf.set("hadoop.job.history.location",new File(workDir).getAbsolutePath()+"/history");
      System.setProperty("hadoop.log.dir", new File(workDir, "/logs").getAbsolutePath());

      mrCluster = new MiniMRCluster(jobTrackerPort,
        taskTrackerPort,
        numTaskTrackers,
        getFileSystem().getUri().toString(),
        numTaskTrackers,
        null,
        null,
        null,
        jobConf);

      jobConf = mrCluster.createJobConf();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to Setup MR Cluster", e);
    }
  }

  private void setupZookeeper() {
    try {
      zookeeperDir = new File(workDir, "zk").getAbsolutePath();
      zookeeperPort = findFreePort();
      zookeeperCluster = new MiniZooKeeperCluster();
      zookeeperCluster.setDefaultClientPort(zookeeperPort);
      zookeeperCluster.startup(new File(zookeeperDir));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to Setup Zookeeper Cluster", e);
    }
  }

  private void setupHBaseCluster() {
    final int numRegionServers = 1;

    try {
      hbaseDir = new File(workDir, "hbase").getCanonicalPath();
      hbaseDir = hbaseDir.replaceAll("\\\\", "/");
      hbaseRoot = "file:///" + hbaseDir;

      if (hbaseConf == null)
        hbaseConf = HBaseConfiguration.create();

      hbaseConf.set("hbase.rootdir", hbaseRoot);
      hbaseConf.set("hbase.master", "local");
      hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort);
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
      hbaseConf.setInt("hbase.master.port", findFreePort());
      hbaseConf.setInt("hbase.master.info.port", -1);
      hbaseConf.setInt("hbase.regionserver.port", findFreePort());
      hbaseConf.setInt("hbase.regionserver.info.port", -1);

      hbaseCluster = new MiniHBaseCluster(hbaseConf, numRegionServers);
      hbaseConf.set("hbase.master", hbaseCluster.getMaster().getServerName().getHostAndPort());
      //opening the META table ensures that cluster is running
      new HTable(hbaseConf, HConstants.META_TABLE_NAME);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to setup HBase Cluster", e);
    }
  }

  private void setUpMetastore() throws Exception {
    if (hiveConf == null)
      hiveConf = new HiveConf(this.getClass());

    //The default org.apache.hadoop.hive.ql.hooks.PreExecutePrinter hook
    //is present only in the ql/test directory
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
      "jdbc:derby:" + new File(workDir + "/metastore_db") + ";create=true");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.toString(),
      new File(workDir, "warehouse").toString());
    //set where derby logs
    File derbyLogFile = new File(workDir + "/derby.log");
    derbyLogFile.createNewFile();
    System.setProperty("derby.stream.error.file", derbyLogFile.getPath());


//    Driver driver = new Driver(hiveConf);
//    SessionState.start(new CliSessionState(hiveConf));

    hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
  }

  private static int findFreePort() throws IOException {
    ServerSocket server = new ServerSocket(0);
    int port = server.getLocalPort();
    server.close();
    return port;
  }

  public static class Builder {
    private File workDir;
    private int numTaskTrackers = 1;
    private JobConf jobConf;
    private Configuration hbaseConf;
    private HiveConf hiveConf;

    private boolean miniMRClusterEnabled = true;
    private boolean miniHBaseClusterEnabled = true;
    private boolean miniHiveMetastoreEnabled = true;
    private boolean miniZookeeperClusterEnabled = true;


    private Builder(File workDir) {
      this.workDir = workDir;
    }

    public Builder numTaskTrackers(int num) {
      numTaskTrackers = num;
      return this;
    }

    public Builder jobConf(JobConf jobConf) {
      this.jobConf = jobConf;
      return this;
    }

    public Builder hbaseConf(Configuration hbaseConf) {
      this.hbaseConf = hbaseConf;
      return this;
    }

    public Builder hiveConf(HiveConf hiveConf) {
      this.hiveConf = hiveConf;
      return this;
    }

    public Builder miniMRClusterEnabled(boolean enabled) {
      this.miniMRClusterEnabled = enabled;
      return this;
    }

    public Builder miniHBaseClusterEnabled(boolean enabled) {
      this.miniHBaseClusterEnabled = enabled;
      return this;
    }

    public Builder miniZookeeperClusterEnabled(boolean enabled) {
      this.miniZookeeperClusterEnabled = enabled;
      return this;
    }

    public Builder miniHiveMetastoreEnabled(boolean enabled) {
      this.miniHiveMetastoreEnabled = enabled;
      return this;
    }


    public ManyMiniCluster build() {
      return new ManyMiniCluster(this);
    }

  }
}
