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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for HBase Tests which need a mini cluster instance
 */
public abstract class SkeletonHBaseTest {

  protected static String TEST_DIR = "/tmp/build/test/data/";

  protected final static String DEFAULT_CONTEXT_HANDLE = "default";

  protected static Map<String, Context> contextMap = new HashMap<String, Context>();
  protected static Set<String> tableNames = new HashSet<String>();

  /**
   * Allow tests to alter the default MiniCluster configuration.
   * (requires static initializer block as all setup here is static)
   */
  protected static Configuration testConf = null;

  protected void createTable(String tableName, String[] families) {
    try {
      HBaseAdmin admin = new HBaseAdmin(getHbaseConf());
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      for (String family : families) {
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
        tableDesc.addFamily(columnDescriptor);
      }
      admin.createTable(tableDesc);
    } catch (Exception e) {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }

  }

  protected String newTableName(String prefix) {
    String name = null;
    int tries = 100;
    do {
      name = prefix + "_" + Math.abs(new Random().nextLong());
    } while (tableNames.contains(name) && --tries > 0);
    if (tableNames.contains(name))
      throw new IllegalStateException("Couldn't find a unique table name, tableNames size: " + tableNames.size());
    tableNames.add(name);
    return name;
  }


  /**
   * startup an hbase cluster instance before a test suite runs
   */
  @BeforeClass
  public static void setup() {
    if (!contextMap.containsKey(getContextHandle()))
      contextMap.put(getContextHandle(), new Context(getContextHandle()));

    contextMap.get(getContextHandle()).start();
  }

  /**
   * shutdown an hbase cluster instance ant the end of the test suite
   */
  @AfterClass
  public static void tearDown() {
    contextMap.get(getContextHandle()).stop();
  }

  /**
   * override this with a different context handle if tests suites are run simultaneously
   * and ManyMiniCluster instances shouldn't be shared
   * @return
   */
  public static String getContextHandle() {
    return DEFAULT_CONTEXT_HANDLE;
  }

  /**
   * @return working directory for a given test context, which normally is a test suite
   */
  public String getTestDir() {
    return contextMap.get(getContextHandle()).getTestDir();
  }

  /**
   * @return ManyMiniCluster instance
   */
  public ManyMiniCluster getCluster() {
    return contextMap.get(getContextHandle()).getCluster();
  }

  /**
   * @return configuration of MiniHBaseCluster
   */
  public Configuration getHbaseConf() {
    return contextMap.get(getContextHandle()).getHbaseConf();
  }

  /**
   * @return configuration of MiniMRCluster
   */
  public Configuration getJobConf() {
    return contextMap.get(getContextHandle()).getJobConf();
  }

  /**
   * @return configuration of Hive Metastore
   */
  public HiveConf getHiveConf() {
    return contextMap.get(getContextHandle()).getHiveConf();
  }

  /**
   * @return filesystem used by ManyMiniCluster daemons
   */
  public FileSystem getFileSystem() {
    return contextMap.get(getContextHandle()).getFileSystem();
  }

  /**
   * class used to encapsulate a context which is normally used by
   * a single TestSuite or across TestSuites when multi-threaded testing is turned on
   */
  public static class Context {
    protected String testDir;
    protected ManyMiniCluster cluster;

    protected Configuration hbaseConf;
    protected Configuration jobConf;
    protected HiveConf hiveConf;

    protected FileSystem fileSystem;

    protected int usageCount = 0;

    public Context(String handle) {
      try {
        testDir = new File(TEST_DIR + "/test_" + handle + "_" + Math.abs(new Random().nextLong()) + "/").getCanonicalPath();
        System.out.println("Cluster work directory: " + testDir);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to generate testDir", e);
      }
    }

    public void start() {
      if (usageCount++ == 0) {
        ManyMiniCluster.Builder b = ManyMiniCluster.create(new File(testDir));
        if (testConf != null) {
          b.hbaseConf(HBaseConfiguration.create(testConf));
        }
        cluster = b.build();
        cluster.start();
        this.hbaseConf = cluster.getHBaseConf();
        jobConf = cluster.getJobConf();
        fileSystem = cluster.getFileSystem();
        hiveConf = cluster.getHiveConf();
      }
    }

    public void stop() {
      if (--usageCount == 0) {
        try {
          cluster.stop();
          cluster = null;
        } finally {
          System.out.println("Trying to cleanup: " + testDir);
          try {
            FileSystem fs = FileSystem.get(jobConf);
            fs.delete(new Path(testDir), true);
          } catch (IOException e) {
            throw new IllegalStateException("Failed to cleanup test dir", e);
          }

        }
      }
    }

    public String getTestDir() {
      return testDir;
    }

    public ManyMiniCluster getCluster() {
      return cluster;
    }

    public Configuration getHbaseConf() {
      return hbaseConf;
    }

    public Configuration getJobConf() {
      return jobConf;
    }

    public HiveConf getHiveConf() {
      return hiveConf;
    }

    public FileSystem getFileSystem() {
      return fileSystem;
    }
  }

}
