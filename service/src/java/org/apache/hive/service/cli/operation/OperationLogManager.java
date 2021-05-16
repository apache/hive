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

package org.apache.hive.service.cli.operation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.cli.session.SessionManager;

/**
 * Move the operation log into another log location that different from the dir created by
 * {@link HiveSessionImpl#setOperationLogSessionDir(File)},
 * this will avoid the operation log being cleaned when session/operation is closed, refer to
 * {@link HiveSessionImpl#close()}, so we can get the operation log for the optimization
 * and investigating the problem of the operation handily for users or administrators.
 * The tree under the log location looks like:
 * - ${@link SessionManager#operationLogRootDir}_historic
 *    - sessionId
 *        - queryId (the operation log file)
 * <p>
 * while the origin tree would like:
 * - ${@link SessionManager#operationLogRootDir}
 *    - sessionId
 *        - queryId (the operation log file)
 * <p>
 * The lifecycle of the log is managed by a daemon called {@link OperationLogDirCleaner},
 * it gets all query info stored in {@link QueryInfoCache}, searches for the query info that can not be reached on the webui,
 * and removes the log. If the operation log session directory has no operation log under it and the session is dead,
 * then the OperationLogDirCleaner will try to cleanup the session log directory.
 */

public class OperationLogManager {
  private static final Logger LOG = LoggerFactory.getLogger(OperationLogManager.class);
  private static final String HISTORIC_DIR_SUFFIX = "_historic";
  private static String HISTORIC_OPERATION_LOG_ROOT_DIR;
  private static long MAX_BYTES_TO_FETCH;

  private final HiveConf hiveConf;
  private final SessionManager sessionManager;
  private final OperationManager operationManager;
  private OperationLogDirCleaner cleaner;

  public OperationLogManager(SessionManager sessionManager, HiveConf hiveConf) {
    this.operationManager = sessionManager.getOperationManager();
    this.hiveConf = hiveConf;
    this.sessionManager = sessionManager;
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_ENABLED)
        && hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)
        && hiveConf.isWebUiQueryInfoCacheEnabled()) {
      initHistoricOperationLogRootDir();
      MAX_BYTES_TO_FETCH = HiveConf.getSizeVar(hiveConf,
          HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_FETCH_MAXBYTES);
      if (HISTORIC_OPERATION_LOG_ROOT_DIR != null
          && !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IN_TEST)) {
        cleaner = new OperationLogDirCleaner();
        cleaner.start();
      }
    }
  }

  private void initHistoricOperationLogRootDir() {
    String originalLogLoc = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION);
    String historicLogLoc = originalLogLoc + HISTORIC_DIR_SUFFIX;
    File operationLogRootDir = new File(historicLogLoc);

    if (operationLogRootDir.exists() && !operationLogRootDir.isDirectory()) {
      LOG.warn("The historic operation log root directory exists, but it is not a directory: " +
          operationLogRootDir.getAbsolutePath());
      return;
    }

    if (!operationLogRootDir.exists()) {
      if (!operationLogRootDir.mkdirs()) {
        LOG.warn("Unable to create historic operation log root directory: " +
            operationLogRootDir.getAbsolutePath());
        return;
      }
    }
    HISTORIC_OPERATION_LOG_ROOT_DIR = historicLogLoc;
  }

  public static OperationLog createOperationLog(Operation operation, QueryState queryState) {
    HiveSession session = operation.getParentSession();
    File parentFile = session.getOperationLogSessionDir();
    boolean isHistoricLogEnabled = HISTORIC_OPERATION_LOG_ROOT_DIR != null;
    if (isHistoricLogEnabled && operation instanceof SQLOperation) {
      String sessionId = session.getSessionHandle().getHandleIdentifier().toString();
      parentFile = new File(HISTORIC_OPERATION_LOG_ROOT_DIR + "/" + sessionId);
      if (!parentFile.exists()) {
        if (!parentFile.mkdirs()) {
          LOG.warn("Unable to create the historic operation log session dir: " + parentFile +
              ", fall back to the original operation log session dir.");
          parentFile = session.getOperationLogSessionDir();
          isHistoricLogEnabled = false;
        }
      } else if (!parentFile.isDirectory()) {
        LOG.warn("The historic operation log session dir: " + parentFile + " is exist, but it's not a directory, " +
            "fall back to the original operation log session dir.");
        parentFile = session.getOperationLogSessionDir();
        isHistoricLogEnabled = false;
      }
    }

    OperationHandle opHandle = operation.getHandle();
    File operationLogFile = new File(parentFile, queryState.getQueryId());
    OperationLog operationLog;
    HiveConf.setBoolVar(queryState.getConf(),
        HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_ENABLED, isHistoricLogEnabled);
    if (isHistoricLogEnabled) {
      // dynamically setting the log location to route the operation log
      HiveConf.setVar(queryState.getConf(),
          HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION, HISTORIC_OPERATION_LOG_ROOT_DIR);
      if (HiveConf.getBoolVar(queryState.getConf(), HiveConf.ConfVars.HIVE_IN_TEST)) {
        HiveConf.setBoolVar(queryState.getConf(), HiveConf.ConfVars.HIVE_TESTING_REMOVE_LOGS, false);
      }
      LOG.info("The operation log location changes from {} to {}.", new File(session.getOperationLogSessionDir(),
          queryState.getQueryId()), operationLogFile);
    }
    operationLog = new OperationLog(opHandle.toString(), operationLogFile, queryState.getConf());
    return operationLog;
  }

  private Set<String> getLiveSessions() {
    Collection<HiveSession> hiveSessions = sessionManager.getSessions();
    Set<String> liveSessions = new HashSet<>();
    for (HiveSession session : hiveSessions) {
      liveSessions.add(session.getSessionHandle().getHandleIdentifier().toString());
    }
    return liveSessions;
  }

  private Set<String> getHistoricSessions(String logRootDir) {
    File logDir = new File(logRootDir);
    Set<String> results = new HashSet<>();
    if (logDir.exists() && logDir.isDirectory()) {
      File[] subFiles = logDir.listFiles();
      if (subFiles != null) {
        for (File f : subFiles) {
          results.add(f.getName());
        }
      }
    }
    return results;
  }


  @VisibleForTesting
  public List<File> getExpiredOperationLogFiles() {
    if (HISTORIC_OPERATION_LOG_ROOT_DIR == null) {
      return Collections.emptyList();
    }

    List<File> results = new ArrayList<>();
    Collection<File> files = FileUtils.listFiles(new File(HISTORIC_OPERATION_LOG_ROOT_DIR)
        , null, true);
    Set<String> queryIds = operationManager.getAllCachedQueryIds();
    for (File logFile : files) {
      if (queryIds.contains(logFile.getName())) {
        continue;
      }
      // if the query info is not cached,
      // add the corresponding historic operation log file into the results.
      results.add(logFile);
    }
    return results;
  }

  @VisibleForTesting
  public List<File> getExpiredSessionLogDirs() {
    if (HISTORIC_OPERATION_LOG_ROOT_DIR == null) {
      return Collections.emptyList();
    }
    List<File> results = new ArrayList<>();
    // go through the original log root dir and historic log root dir for dead sessions
    Set<String> liveSessions = getLiveSessions();
    Set<String> historicSessions = getHistoricSessions(HISTORIC_OPERATION_LOG_ROOT_DIR);
    historicSessions.removeAll(liveSessions);
    Set<String> queryIds = operationManager.getAllCachedQueryIds();
    // add the historic log session dir into the results if the session is dead and
    // no historic operation log under the dir
    for (String sessionId : historicSessions) {
      File sessionLogDir = new File(HISTORIC_OPERATION_LOG_ROOT_DIR, sessionId);
      if (sessionLogDir.exists()) {
        File[] logFiles = sessionLogDir.listFiles();
        if (logFiles == null || logFiles.length == 0) {
          results.add(sessionLogDir);
        } else {
          boolean found = false;
          for (File logFile : logFiles) {
            if (queryIds.contains(logFile.getName())) {
              found = true;
              break;
            }
          }
          if (!found) {
            results.add(sessionLogDir);
          }
        }
      }
    }
    return results;
  }

  private List<String> getFileNames(List<File> fileList) {
    List<String> results = new ArrayList<>();
    for (File file : fileList) {
      results.add(file.getName());
    }
    return results;
  }

  @VisibleForTesting
  public void removeExpiredOperationLogAndDir() {
    if (HISTORIC_OPERATION_LOG_ROOT_DIR == null) {
      return;
    }
    // remove the expired operation logs firstly
    List<File> operationLogFiles = getExpiredOperationLogFiles();
    if (operationLogFiles.isEmpty()) {
      LOG.info("No expired operation logs found under the dir: {}", HISTORIC_OPERATION_LOG_ROOT_DIR);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to delete the expired operation logs: {} ", getFileNames(operationLogFiles));
      }
      for (File logFile : operationLogFiles) {
        FileUtils.deleteQuietly(logFile);
      }
      LOG.info("Deleted {} expired operation logs", operationLogFiles.size());
    }
    // remove the historic operation log session dirs
    List<File> sessionLogDirs = getExpiredSessionLogDirs();
    if (sessionLogDirs.isEmpty()) {
      LOG.info("No expired operation log session dir under the dir: {}", HISTORIC_OPERATION_LOG_ROOT_DIR);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to delete the expired operation log session dirs: {} ", getFileNames(sessionLogDirs));
      }
      for (File logDir : sessionLogDirs) {
        FileUtils.deleteQuietly(logDir);
      }
      LOG.info("Deleted {} expired operation log session dirs", sessionLogDirs.size());
    }
  }

  private class OperationLogDirCleaner extends Thread {
    private final long interval;
    private boolean shutdown = false;
    private final Object monitor = new Object();

    OperationLogDirCleaner() {
      long checkInterval = HiveConf.getTimeVar(hiveConf,
          HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
      this.interval = Math.max(checkInterval, 3000l);
      setName("Historic-OperationLogDir-Cleaner");
      setDaemon(true);
    }

    @Override
    public void run() {
      sleepFor(interval);
      while (!shutdown) {
        try {
          removeExpiredOperationLogAndDir();
          sleepFor(interval);
        } catch (Exception e) {
          LOG.warn("OperationLogDir cleaner caught exception: " + e.getMessage(), e);
        }
      }
    }

    private void sleepFor(long interval) {
      synchronized (monitor) {
        if (shutdown) {
          return;
        }
        try {
          monitor.wait(interval);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }

    void shutDown() {
      synchronized (monitor) {
        shutdown = true;
        monitor.notifyAll();
      }
    }

  }

  public void stop() {
    if (cleaner != null) {
      cleaner.shutDown();
    }
  }

  private static boolean isHistoricOperationLogEnabled(String logLocation) {
    if (logLocation == null || HISTORIC_OPERATION_LOG_ROOT_DIR == null) {
      return false;
    }
    return logLocation.startsWith(HISTORIC_OPERATION_LOG_ROOT_DIR);
  }

  public static String getOperationLog(QueryInfo queryInfo) {
    String logLocation = queryInfo.getOperationLogLocation();
    StringBuilder builder = new StringBuilder();
    if (!isHistoricOperationLogEnabled(logLocation)) {
      if (logLocation == null) {
        return "Operation log is disabled, please set hive.server2.logging.operation.enabled = true to enable it";
      }
      builder.append("Operation Log - will be deleted after query completes, ")
          .append("set hive.server2.historic.operation.log.enabled = true ")
          .append("and hive.server2.webui.max.historic.queries > 0 to disable it")
          .append(System.lineSeparator());
    }

    try (RandomAccessFile raf = new RandomAccessFile(logLocation, "r")) {
      long fileLen = raf.length();
      // try to fetch the latest logs
      long seekPos = 0;
      if (fileLen > MAX_BYTES_TO_FETCH) {
        seekPos = fileLen - MAX_BYTES_TO_FETCH;
      }
      ByteBuffer buffer = ByteBuffer.allocate((int) MAX_BYTES_TO_FETCH);
      int read = raf.getChannel().read(buffer, seekPos);
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.array(), 0, read)))) {
        String line;
        while ((line = reader.readLine()) != null) {
          builder.append(line).append(System.lineSeparator());
        }
      }
    } catch (Exception e) {
      builder.append(StringUtils.stringifyException(e));
    }

    return builder.toString();
  }

  @VisibleForTesting
  public static String getHistoricLogDir() {
    return HISTORIC_OPERATION_LOG_ROOT_DIR;
  }

}
