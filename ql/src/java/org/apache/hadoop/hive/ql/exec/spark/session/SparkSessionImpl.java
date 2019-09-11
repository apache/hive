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
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Implementation of {@link SparkSession} that treats each Spark session as a separate Spark
 * application.
 *
 * <p>
 *   It uses a {@link HiveSparkClient} to submit a Spark application and to submit Spark jobs to
 *   the Spark app.
 * </p>
 *
 * <p>
 *   This class contains logic to trigger a timeout of this {@link SparkSession} if certain
 *   conditions are met (e.g. a job hasn't been submitted in the past "x" seconds). Since we use
 *   a threadpool to schedule a task that regularly checks if a session has timed out, we need to
 *   properly synchronize the {@link #open(HiveConf)} and {@link #close()} methods. We use a
 *   series of volatile variables and read-write locks to ensure this.
 * </p>
 */
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

  // Several of the following variables need to be volatile so they can be accessed by the timeout
  // thread

  private volatile HiveConf conf;
  private volatile boolean isOpen;
  private final String sessionId;
  private volatile HiveSparkClient hiveSparkClient;
  private volatile Path scratchDir;

  /**
   * The timestamp of the last completed Spark job.
   */
  private volatile long lastSparkJobCompletionTime;

  /**
   * A {@link Set} of currently running queries. Each job is identified by its query id.
   */
  private final Set<String> activeJobs = Sets.newConcurrentHashSet();

  private ReadWriteLock closeLock = new ReentrantReadWriteLock();

  SparkSessionImpl(String sessionId) {
    this.sessionId = sessionId;
    initErrorPatterns();
  }

  @Override
  public void open(HiveConf conf) throws HiveException {
    closeLock.writeLock().lock();

    try {
      if (!isOpen) {
        LOG.info("Trying to open Hive on Spark session {}", sessionId);
        this.conf = conf;

        try {
          hiveSparkClient = HiveSparkClientFactory.createHiveSparkClient(conf, sessionId,
                  SessionState.get().getSessionId());
          isOpen = true;
        } catch (Throwable e) {
          throw getHiveException(e);
        }
        LOG.info("Hive on Spark session {} successfully opened", sessionId);
      } else {
        LOG.info("Hive on Spark session {} is already opened", sessionId);
      }
    } finally {
      closeLock.writeLock().unlock();
    }
  }

  @Override
  public SparkJobRef submit(DriverContext driverContext, SparkWork sparkWork) throws Exception {
    closeLock.readLock().lock();
    try {
      Preconditions.checkState(isOpen, "Hive on Spark session is not open. Can't submit jobs.");
      return hiveSparkClient.execute(driverContext, sparkWork);
    } finally {
      closeLock.readLock().unlock();
    }
  }

  @Override
  public Pair<Long, Integer> getMemoryAndCores() throws Exception {
    closeLock.readLock().lock();
    try {
      SparkConf sparkConf = hiveSparkClient.getSparkConf();
      int numExecutors = hiveSparkClient.getExecutorCount();
      // at start-up, we may be unable to get number of executors
      if (numExecutors <= 0) {
        return Pair.of(-1L, -1);
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
      LOG.info("Hive on Spark application currently has number of executors: " + numExecutors
              + ", total cores: " + totalCores + ", memory per executor: "
              + executorMemoryInMB + " mb, memoryFraction: " + memoryFraction);
      return Pair.of(Long.valueOf(memoryPerTaskInBytes), Integer.valueOf(totalCores));
    } finally {
      closeLock.readLock().unlock();
    }
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
    if (isOpen) {
      closeLock.writeLock().lock();

      try {
        if (isOpen) {
          LOG.info("Trying to close Hive on Spark session {}", sessionId);
          if (hiveSparkClient != null) {
            try {
              hiveSparkClient.close();
              LOG.info("Hive on Spark session {} successfully closed", sessionId);
              cleanScratchDir();
            } catch (IOException e) {
              LOG.error("Failed to close Hive on Spark session (" + sessionId + ")", e);
            }
          }
          hiveSparkClient = null;
          lastSparkJobCompletionTime = 0;

          isOpen = false;
        }
      } finally {
        closeLock.writeLock().unlock();
      }
    }
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
    StringBuilder matchedString = new StringBuilder();
    while (e != null) {
      if (e instanceof TimeoutException) {
        return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_TIMEOUT, sessionId);
      } else if (e instanceof InterruptedException) {
        return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_INTERRUPTED, sessionId);
      } else if (e instanceof RuntimeException) {
        String sts = Throwables.getStackTraceAsString(e);
        if (matches(sts, AM_TIMEOUT_ERR, matchedString)) {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_TIMEOUT, sessionId);
        } else if (matches(sts, UNKNOWN_QUEUE_ERR, matchedString) || matches(sts, STOPPED_QUEUE_ERR, matchedString)) {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_INVALID_QUEUE, sessionId,
                  matchedString.toString());
        } else if (matches(sts, FULL_QUEUE_ERR, matchedString)) {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_QUEUE_FULL, sessionId,
                  matchedString.toString());
        } else if (matches(sts, INVALILD_MEM_ERR, matchedString) || matches(sts, INVALID_CORE_ERR, matchedString)) {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_INVALID_RESOURCE_REQUEST,
              sessionId, matchedString.toString());
        } else {
          return new HiveException(e, ErrorMsg.SPARK_CREATE_CLIENT_ERROR, sessionId,
              Throwables.getRootCause(e).toString());
        }
      }
      e = e.getCause();
    }

    return new HiveException(oe, ErrorMsg.SPARK_CREATE_CLIENT_ERROR, sessionId,
        Throwables.getRootCause(oe).toString());
  }

  private boolean matches(String input, String regex, StringBuilder matchedString) {
    if (!errorPatterns.containsKey(regex)) {
      LOG.warn("No error pattern found for regex: {}", regex);
      return false;
    }
    Pattern p = errorPatterns.get(regex);
    Matcher m = p.matcher(input);
    boolean result = m.find();
    if (result && m.groupCount() == 1) {
      // assume matchedString is empty
      matchedString.append(m.group(1));
    }
    return result;
  }

  //This method is not thread safe
  private void cleanScratchDir() throws IOException {
    if (scratchDir != null) {
      FileSystem fs = scratchDir.getFileSystem(conf);
      fs.delete(scratchDir, true);
      scratchDir = null;
    }
  }
  /**
   * Create scratch directory for spark session if it does not exist.
   * This method is not thread safe.
   * @return Path to Spark session scratch directory.
   * @throws IOException
   */
  @Override
  public Path getHDFSSessionDir() throws IOException {
    if (scratchDir == null) {
      scratchDir = createScratchDir();
    }
    return scratchDir;
  }

  @Override
  public void onQuerySubmission(String queryId) {
    activeJobs.add(queryId);
  }

  /**
   * Check if a session has timed out, and if it has close the session.
   */
  @Override
  public boolean triggerTimeout(long sessionTimeout) {
    if (hasTimedOut(activeJobs, lastSparkJobCompletionTime, sessionTimeout)) {
      closeLock.writeLock().lock();

      try {
        if (hasTimedOut(activeJobs, lastSparkJobCompletionTime, sessionTimeout)) {
          LOG.warn("Closing Spark session " + getSessionId() + " because a Spark job has not " +
                  "been run in the past " + sessionTimeout / 1000 + " seconds");
          close();
          return true;
        }
      } finally {
        closeLock.writeLock().unlock();
      }
    }
    return false;
  }

  /**
   * Returns true if a session has timed out, false otherwise. The following conditions must be met
   * in order to consider a session as timed out:
   * (1) the session must have run at least one query (i.e. lastSparkJobCompletionTime > 0),
   * (2) there can be no actively running Spark jobs, and
   * (3) the last completed Spark job must have been more than sessionTimeout seconds ago.
   */
  private static boolean hasTimedOut(Set<String> activeJobs,
                                     long lastSparkJobCompletionTime, long sessionTimeout) {
    return activeJobs.isEmpty() &&
            lastSparkJobCompletionTime > 0 &&
            (System.currentTimeMillis() - lastSparkJobCompletionTime) > sessionTimeout;
  }

  /**
   * When this session completes the execution of a query, remove the query from the list of actively running jobs,
   * and set the {@link #lastSparkJobCompletionTime} to the current timestamp.
   */
  @Override
  public void onQueryCompletion(String queryId) {
    activeJobs.remove(queryId);
    lastSparkJobCompletionTime = System.currentTimeMillis();
  }

  @VisibleForTesting
  HiveSparkClient getHiveSparkClient() {
    return hiveSparkClient;
  }
}
