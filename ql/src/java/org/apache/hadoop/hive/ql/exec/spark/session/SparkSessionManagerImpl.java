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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClientFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.hive.spark.client.SparkClientFactory;

/**
 * Simple implementation of <i>SparkSessionManager</i>
 *   - returns SparkSession when requested through <i>getSession</i> and keeps track of
 *       created sessions. Currently no limit on the number sessions.
 *   - SparkSession is reused if the userName in new conf and user name in session conf match.
 */
public class SparkSessionManagerImpl implements SparkSessionManager {
  private static final Logger LOG = LoggerFactory.getLogger(SparkSessionManagerImpl.class);

  private final Set<SparkSession> createdSessions = Sets.newConcurrentHashSet();

  /**
   * A {@link Future} that tracks the status of the scheduled time out thread launched via the
   * {@link #startTimeoutThread()} method.
   */
  private volatile Future<?> timeoutFuture;

  private volatile boolean inited = false;
  private volatile HiveConf conf;

  private static SparkSessionManagerImpl instance;

  static {
    ShutdownHookManager.addShutdownHook(new Runnable() {
      @Override
      public void run() {
        try {
          if (instance != null) {
            instance.shutdown();
          }
        } catch (Exception e) {
          // ignore
        }
      }
    });
  }

  public static synchronized SparkSessionManagerImpl getInstance()
      throws HiveException {
    if (instance == null) {
      instance = new SparkSessionManagerImpl();
    }

    return instance;
  }

  private SparkSessionManagerImpl() {
  }

  @Override
  public void setup(HiveConf hiveConf) throws HiveException {
    if (!inited) {
      synchronized (this) {
        if (!inited) {
          LOG.info("Setting up the session manager.");
          conf = hiveConf;
          startTimeoutThread();
          Map<String, String> sparkConf = HiveSparkClientFactory.initiateSparkConf(hiveConf, null);
          try {
            SparkClientFactory.initialize(sparkConf, hiveConf);
            inited = true;
          } catch (IOException e) {
            throw new HiveException("Error initializing SparkClientFactory", e);
          }
        }
      }
    }
  }

  /**
   * If the <i>existingSession</i> can be reused return it.
   * Otherwise
   *   - close it and remove it from the list.
   *   - create a new session and add it to the list.
   */
  @Override
  public SparkSession getSession(SparkSession existingSession, HiveConf conf, boolean doOpen)
      throws HiveException {
    setup(conf);

    if (existingSession != null) {
      // Open the session if it is closed.
      if (!existingSession.isOpen() && doOpen) {
        existingSession.open(conf);
        createdSessions.add(existingSession);
      }
      return existingSession;
    }

    SparkSession sparkSession = new SparkSessionImpl(SessionState.get().getNewSparkSessionId());
    if (doOpen) {
      sparkSession.open(conf);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("New session (%s) is created.", sparkSession.getSessionId()));
    }
    createdSessions.add(sparkSession);
    return sparkSession;
  }

  @Override
  public void returnSession(SparkSession sparkSession) throws HiveException {
    // In this particular SparkSessionManager implementation, we don't recycle
    // returned sessions. References to session are still valid.
  }

  @Override
  public void closeSession(SparkSession sparkSession) throws HiveException {
    if (sparkSession == null) {
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Closing Spark session (%s).", sparkSession.getSessionId()));
    }
    sparkSession.close();
    createdSessions.remove(sparkSession);
  }

  @Override
  public void shutdown() {
    LOG.info("Closing the session manager.");
    if (timeoutFuture != null) {
      timeoutFuture.cancel(false);
    }
    createdSessions.forEach(SparkSession::close);
    createdSessions.clear();
    inited = false;
    SparkClientFactory.stop();
  }

  /**
   * Starts a scheduled thread that periodically calls {@link SparkSession#triggerTimeout(long)}
   * on each {@link SparkSession} managed by this class.
   */
  private void startTimeoutThread() {
    long sessionTimeout = conf.getTimeVar(HiveConf.ConfVars.SPARK_SESSION_TIMEOUT,
            TimeUnit.MILLISECONDS);
    long sessionTimeoutPeriod = conf.getTimeVar(HiveConf.ConfVars.SPARK_SESSION_TIMEOUT_PERIOD,
            TimeUnit.MILLISECONDS);
    ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();

    // Schedules a thread that does the following: iterates through all the active SparkSessions
    // and calls #triggerTimeout(long) on each one. If #triggerTimeout(long) returns true, then
    // the SparkSession is removed from the set of active sessions managed by this class.
    timeoutFuture = es.scheduleAtFixedRate(() -> createdSessions.stream()
                    .filter(sparkSession -> sparkSession.triggerTimeout(sessionTimeout))
                    .forEach(createdSessions::remove),
            0, sessionTimeoutPeriod, TimeUnit.MILLISECONDS);
  }
}
