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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClientFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.spark.client.SparkClientFactory;

/**
 * Simple implementation of <i>SparkSessionManager</i>
 *   - returns SparkSession when requested through <i>getSession</i> and keeps track of
 *       created sessions. Currently no limit on the number sessions.
 *   - SparkSession is reused if the userName in new conf and user name in session conf match.
 */
public class SparkSessionManagerImpl implements SparkSessionManager {
  private static final Logger LOG = LoggerFactory.getLogger(SparkSessionManagerImpl.class);

  private Set<SparkSession> createdSessions = Collections.synchronizedSet(new HashSet<SparkSession>());
  private volatile boolean inited = false;

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
          Map<String, String> conf = HiveSparkClientFactory.initiateSparkConf(hiveConf);
          try {
            SparkClientFactory.initialize(conf);
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
      }
      return existingSession;
    }

    SparkSession sparkSession = new SparkSessionImpl();
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
      LOG.debug(String.format("Closing session (%s).", sparkSession.getSessionId()));
    }
    sparkSession.close();
    createdSessions.remove(sparkSession);
  }

  @Override
  public void shutdown() {
    LOG.info("Closing the session manager.");
    synchronized (createdSessions) {
      Iterator<SparkSession> it = createdSessions.iterator();
      while (it.hasNext()) {
        SparkSession session = it.next();
        session.close();
      }
      createdSessions.clear();
    }
    inited = false;
    SparkClientFactory.stop();
  }
}
