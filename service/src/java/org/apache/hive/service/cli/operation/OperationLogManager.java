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
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.common.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.cli.session.SessionManager;

/**
 * Move the operation log into another log location that different from the dir created by
 * {@link HiveSessionImpl#setOperationLogSessionDir(File)},
 * this will avoid the operation log being cleaned when session/operation is closed, refer to
 * {@link HiveSessionImpl#close()}, so we can get the operation log handy for further optimization
 * and investigation after query completes.
 * The tree under the log location looks like:
 * - ${@link SessionManager#operationLogRootDir}_historic
 *    - hostname_thriftPort_startTime
 *       - sessionId
 *          - queryId (the operation log file)
 * <p>
 * while the origin tree would like:
 * - ${@link SessionManager#operationLogRootDir}
 *    - sessionId
 *       - queryId (the operation log file)
 * <p>
 * The removals of the operation log and log session dir are managed by a daemon called {@link OperationLogDirCleaner},
 * it scans through the historic log root dir for the expired operation logs, the operation log is being expired
 * and can be removed when the operation's query info does not cached in  {@link QueryInfoCache} and cannot be found
 * on the webui. If the log session dir has no operation logs under it and the session is closed,
 * then the cleaner will cleanup the log session dir.
 */

public class OperationLogManager {
  private static final Logger LOG = LoggerFactory.getLogger(OperationLogManager.class);
  private static final String HISTORIC_DIR_SUFFIX = "_historic";
  private static String historicLogRootDir;
  private static long maxBytesToFetch;

  private final HiveConf hiveConf;
  private final OperationManager operationManager;
  private OperationLogDirCleaner cleaner;
  private String historicParentLogDir;
  private String serverInstance;

  public OperationLogManager(OperationManager operationManager, HiveConf hiveConf) {
    this.operationManager = operationManager;
    this.hiveConf = hiveConf;
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_ENABLED)
        && hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)
        && hiveConf.isWebUiQueryInfoCacheEnabled()) {
      initHistoricOperationLogRootDir();
      maxBytesToFetch = HiveConf.getSizeVar(hiveConf,
          HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_FETCH_MAXBYTES);
      if (historicLogRootDir != null
          && !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IN_TEST)) {
        cleaner = new OperationLogDirCleaner();
        cleaner.start();
      }
    }
  }

  private String getServerInstance() {
    String hostname;
    try {
      hostname = ServerUtils.hostname();
    } catch (Exception e) {
      // A random id is given on exception
      hostname = UUID.randomUUID().toString();
    }
    int serverPort = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
    return hostname + "_" + serverPort;
  }

  private void initHistoricOperationLogRootDir() {
    String origLogLoc = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION);
    File logLocation = new File(origLogLoc);
    historicParentLogDir = logLocation.getAbsolutePath() + HISTORIC_DIR_SUFFIX;
    serverInstance = getServerInstance();
    String logRootDir = new StringBuilder(historicParentLogDir)
        .append("/").append(serverInstance)
        .append("_").append(System.currentTimeMillis()).toString();
    File operationLogRootDir = new File(logRootDir);

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
    historicLogRootDir = logRootDir;
  }

  // Delete historical query logs that are not in use by Web UI.
  public void deleteHistoricQueryLogs() {
    if (historicLogRootDir == null) {
      return;
    }
    File logDir = new File(historicLogRootDir);
    if (!logDir.exists() || !logDir.isDirectory()) {
      return;
    }
    File[] subDirs = logDir.listFiles();
    if (subDirs == null || subDirs.length==0) {
      return;
    }

    Set<String> sessionIds = operationManager.getHistoricalQueryInfos().stream()
        .map(QueryInfo::getSessionId).collect(Collectors.toSet());
    Set<String> queryIds = operationManager.getHistoricalQueryInfos().stream()
        .map(queryInfo -> queryInfo.getQueryDisplay().getQueryId()).collect(Collectors.toSet());

    for (File dir : subDirs) {
      if (dir.isDirectory()) {
        if (sessionIds.contains(dir.getName())) {
          for (File f : dir.listFiles()) {
            if (!queryIds.contains(f.getName()) ) {
              LOG.debug("delete file not in hist: " + f.getName());
              FileUtils.deleteQuietly(f);
            }
          }
        } else {
          FileUtils.deleteQuietly(dir);
        }
      }
    }
  }

  // delete the older historic log root dirs on restart
  private void deleteElderLogRootDirs() {
    File[] children = new File(historicParentLogDir).listFiles(new FileFilter() {
      @Override
      public boolean accept(File child) {
        return child.isDirectory()
            && child.getName().startsWith(serverInstance)
            && !child.getAbsolutePath().equals(historicLogRootDir);
      }
    });

    if (children == null || children.length == 0) {
      return;
    }

    for (File f : children) {
      FileUtils.deleteQuietly(f);
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
      deleteElderLogRootDirs();
      sleepFor(interval);
      while (!shutdown) {
        try {
          deleteHistoricQueryLogs();
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
    if (logLocation == null || historicLogRootDir == null) {
      return false;
    }
    return logLocation.startsWith(historicLogRootDir);
  }

  public static void closeOperation(Operation operation) {
    String queryId = operation.getQueryId();
    File originOpLogFile = new File(operation.parentSession.getOperationLogSessionDir(), queryId);
    if (!originOpLogFile.exists()) {
      return;
    }
    HiveConf hiveConf = operation.queryState.getConf();
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_HISTORIC_OPERATION_LOG_ENABLED)
        && operation instanceof SQLOperation) {
      String sessionHandle = operation.getParentSession().getSessionHandle().getHandleIdentifier().toString();
      String histOpLogFileLocation = new StringBuilder(historicLogRootDir)
          .append("/").append(sessionHandle)
          .append("/").append(queryId).toString();
      try {
        FileUtils.moveFile(originOpLogFile, new File(histOpLogFileLocation));
        QueryInfo queryInfo = ((SQLOperation) operation).getQueryInfo();
        queryInfo.setOperationLogLocation(histOpLogFileLocation);
        LOG.info("The operation log location changes from {} to {}.", originOpLogFile, histOpLogFileLocation);
      } catch (IOException e) {
        LOG.error("Failed to move operation log location from {} to {}: {}.",
            originOpLogFile, histOpLogFileLocation, e.getMessage());
      }
    } else {
      FileUtils.deleteQuietly(originOpLogFile);
    }
    LOG.debug(queryId + ": closeOperation");
  }

  public static String getOperationLog(QueryInfo queryInfo) {
    String logLocation = queryInfo.getOperationLogLocation();
    StringBuilder builder = new StringBuilder();
    if (logLocation == null) {
      return "Operation log is disabled, please set hive.server2.logging.operation.enabled = true to enable it";
    }
    if (historicLogRootDir == null) {
      builder.append("Operation Log - will be deleted after query completes, ")
          .append("set hive.server2.historic.operation.log.enabled = true ")
          .append("and hive.server2.webui.max.historic.queries > 0 to disable it")
          .append(System.lineSeparator());
    }

    try (RandomAccessFile raf = new RandomAccessFile(logLocation, "r")) {
      long fileLen = raf.length();
      // try to fetch the latest logs
      long seekPos = 0;
      if (fileLen > maxBytesToFetch) {
        seekPos = fileLen - maxBytesToFetch;
      }
      ByteBuffer buffer = ByteBuffer.allocate((int) maxBytesToFetch);
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
    return historicLogRootDir;
  }

}
