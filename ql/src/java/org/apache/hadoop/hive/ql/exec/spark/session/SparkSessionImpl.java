/*
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.ErrorMsg;
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

  /** Regex for different Spark session error messages */
  private static final String AM_TIMEOUT_ERR = ".*ApplicationMaster for attempt.*timed out.*";
  private static final String UNKNOWN_QUEUE_ERR = "(submitted by user.*to unknown queue:.*)\n";
  private static final String STOPPED_QUEUE_ERR = "(Queue.*is STOPPED)";
  private static final String FULL_QUEUE_ERR = "(Queue.*already has.*applications)";
  private static final String INVALILD_MEM_ERR =
      "(Required executor memory.*is above the max threshold.*) of this";
  private static final String INVALID_CORE_ERR =
      "(initial executor number.*must between min executor.*and max executor number.*)\n";

  /** Pre-compiled error patterns. Shared between all Spark sessions */
  private static Map<String, Pattern> errorPatterns;

  private HiveConf conf;
  private boolean isOpen;
  private final String sessionId;
  private HiveSparkClient hiveSparkClient;
  private Path scratchDir;
  private final Object dirLock = new Object();
  private String matchedString = null;

  public SparkSessionImpl() {
    sessionId = makeSessionId();
    initErrorPatterns();
  }

  @Override
  public void open(HiveConf conf) throws HiveException {
    LOG.info("Trying to open Spark session {}", sessionId);
    this.conf = conf;
    isOpen = true;
    try {
      hiveSparkClient = HiveSparkClientFactory.createHiveSparkClient(conf, sessionId);
    } catch (Throwable e) {
      // It's possible that user session is closed while creating Spark client.
      HiveException he;
      if (isOpen) {
        he = getHiveException(e);
      } else {
        he = new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_CLOSED_SESSION, sessionId);
      }
      throw he;
    }
    LOG.info("Spark session {} is successfully opened", sessionId);
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
    if (masterURL.startsWith("spark") || masterURL.startsWith("local")) {
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
    LOG.info("Trying to close Spark session {}", sessionId);
    isOpen = false;
    if (hiveSparkClient != null) {
      try {
        hiveSparkClient.close();
        LOG.info("Spark session {} is successfully closed", sessionId);
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

  private static void initErrorPatterns() {
    errorPatterns = Maps.newHashMap(
        new ImmutableMap.Builder<String, Pattern>()
            .put(AM_TIMEOUT_ERR, Pattern.compile(AM_TIMEOUT_ERR))
            .put(UNKNOWN_QUEUE_ERR, Pattern.compile(UNKNOWN_QUEUE_ERR))
            .put(STOPPED_QUEUE_ERR, Pattern.compile(STOPPED_QUEUE_ERR))
            .put(FULL_QUEUE_ERR, Pattern.compile(FULL_QUEUE_ERR))
            .put(INVALILD_MEM_ERR, Pattern.compile(INVALILD_MEM_ERR))
            .put(INVALID_CORE_ERR, Pattern.compile(INVALID_CORE_ERR))
            .build()
    );
  }

  @VisibleForTesting
  HiveException getHiveException(Throwable e) {
    Throwable oe = e;
    while (e != null) {
      if (e instanceof TimeoutException) {
        return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_TIMEOUT);
      } else if (e instanceof InterruptedException) {
        return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_INTERRUPTED, sessionId);
      } else if (e instanceof RuntimeException) {
        String sts = Throwables.getStackTraceAsString(e);
        if (matches(sts, AM_TIMEOUT_ERR)) {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_TIMEOUT);
        } else if (matches(sts, UNKNOWN_QUEUE_ERR) || matches(sts, STOPPED_QUEUE_ERR)) {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_INVALID_QUEUE, matchedString);
        } else if (matches(sts, FULL_QUEUE_ERR)) {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_QUEUE_FULL, matchedString);
        } else if (matches(sts, INVALILD_MEM_ERR) || matches(sts, INVALID_CORE_ERR)) {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_INVALID_RESOURCE_REQUEST,
              matchedString);
        } else {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_ERROR, sessionId);
        }
      }
      e = e.getCause();
    }

    return new HiveException(oe, ErrorMsg.SPARK_CREATE_CLIENT_ERROR, sessionId);
  }

  @VisibleForTesting
  String getMatchedString() {
    return matchedString;
  }

  private boolean matches(String input, String regex) {
    if (!errorPatterns.containsKey(regex)) {
      LOG.warn("No error pattern found for regex: {}", regex);
      return false;
    }
    Pattern p = errorPatterns.get(regex);
    Matcher m = p.matcher(input);
    boolean result = m.find();
    if (result && m.groupCount() == 1) {
      this.matchedString = m.group(1);
    }
    return result;
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

  @VisibleForTesting
  HiveSparkClient getHiveSparkClient() {
    return hiveSparkClient;
  }
}
