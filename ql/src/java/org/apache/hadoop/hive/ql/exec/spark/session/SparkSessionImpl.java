/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark.session;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClientFactory;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.spark.SparkConf;
import org.apache.spark.util.Utils;

import com.google.common.base.Preconditions;

public class SparkSessionImpl implements SparkSession {
  private static final Logger LOG = LoggerFactory.getLogger(SparkSession.class);
  private static final String SPARK_DIR = "_spark_session_dir";

  private HiveConf conf;
  private boolean isOpen;
  private final String sessionId;
  private HiveSparkClient hiveSparkClient;
  private Path scratchDir;
  private final Object dirLock = new Object();

  public SparkSessionImpl() {
    sessionId = makeSessionId();
  }

  @Override
  public void open(HiveConf conf) throws HiveException {
    this.conf = conf;
    isOpen = true;
    try {
      hiveSparkClient = HiveSparkClientFactory.createHiveSparkClient(conf);
    } catch (Throwable e) {
      throw new HiveException("Failed to create spark client.", e);
    }
  }

  @Override
  public SparkJobRef submit(DriverContext driverContext, SparkWork sparkWork) throws Exception {
    Preconditions.checkState(isOpen, "Session is not open. Can't submit jobs.");
    return hiveSparkClient.execute(driverContext, sparkWork);
  }

  @Override
  public ObjectPair<Long, Integer> getMemoryAndCores() throws Exception {
    SparkConf sparkConf = hiveSparkClient.getSparkConf();
    int numExecutors = hiveSparkClient.getExecutorCount();
    // at start-up, we may be unable to get number of executors
    if (numExecutors <= 0) {
      return new ObjectPair<Long, Integer>(-1L, -1);
    }
    int executorMemoryInMB = Utils.memoryStringToMb(
        sparkConf.get("spark.executor.memory", "512m"));
    double memoryFraction = 1.0 - sparkConf.getDouble("spark.storage.memoryFraction", 0.6);
    long totalMemory = (long) (numExecutors * executorMemoryInMB * memoryFraction * 1024 * 1024);
    int totalCores;
    String masterURL = sparkConf.get("spark.master");
    if (masterURL.startsWith("spark")) {
      totalCores = sparkConf.contains("spark.default.parallelism") ?
          sparkConf.getInt("spark.default.parallelism", 1) :
          hiveSparkClient.getDefaultParallelism();
      totalCores = Math.max(totalCores, numExecutors);
    } else {
      int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
      totalCores = numExecutors * coresPerExecutor;
    }
    totalCores = totalCores / sparkConf.getInt("spark.task.cpus", 1);

    long memoryPerTaskInBytes = totalMemory / totalCores;
    LOG.info("Spark cluster current has executors: " + numExecutors
        + ", total cores: " + totalCores + ", memory per executor: "
        + executorMemoryInMB + "M, memoryFraction: " + memoryFraction);
    return new ObjectPair<Long, Integer>(Long.valueOf(memoryPerTaskInBytes),
        Integer.valueOf(totalCores));
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public HiveConf getConf() {
    return conf;
  }

  @Override
  public String getSessionId() {
    return sessionId;
  }

  @Override
  public void close() {
    isOpen = false;
    if (hiveSparkClient != null) {
      try {
        hiveSparkClient.close();
        cleanScratchDir();
      } catch (IOException e) {
        LOG.error("Failed to close spark session (" + sessionId + ").", e);
      }
    }
    hiveSparkClient = null;
  }

  private Path createScratchDir() throws IOException {
    Path parent = new Path(SessionState.get().getHdfsScratchDirURIString(), SPARK_DIR);
    Path sparkDir = new Path(parent, sessionId);
    FileSystem fs = sparkDir.getFileSystem(conf);
    FsPermission fsPermission = new FsPermission(HiveConf.getVar(
        conf, HiveConf.ConfVars.SCRATCHDIRPERMISSION));
    fs.mkdirs(sparkDir, fsPermission);
    fs.deleteOnExit(sparkDir);
    return sparkDir;
  }

  private void cleanScratchDir() throws IOException {
    if (scratchDir != null) {
      FileSystem fs = scratchDir.getFileSystem(conf);
      fs.delete(scratchDir, true);
      scratchDir = null;
    }
  }

  @Override
  public Path getHDFSSessionDir() throws IOException {
    if (scratchDir == null) {
      synchronized (dirLock) {
        if (scratchDir == null) {
          scratchDir = createScratchDir();
        }
      }
    }
    return scratchDir;
  }

  public static String makeSessionId() {
    return UUID.randomUUID().toString();
  }
}
