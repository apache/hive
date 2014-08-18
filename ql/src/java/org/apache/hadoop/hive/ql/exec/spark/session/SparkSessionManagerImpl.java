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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Simple implementation of <i>SparkSessionManager</i>
 *   - returns SparkSession when requested through <i>getSession</i> and keeps track of
 *       created sessions. Currently no limit on the number sessions.
 *   - SparkSession is reused if the userName in new conf and user name in session conf match.
 */
public class SparkSessionManagerImpl implements SparkSessionManager {
  private static final Log LOG = LogFactory.getLog(SparkSessionManagerImpl.class);

  private Set<SparkSession> createdSessions;
  private boolean inited;

  private static SparkSessionManagerImpl instance;

  static {
    Runtime.getRuntime().addShutdownHook(new Thread() {
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

  public synchronized static SparkSessionManagerImpl getInstance()
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
    LOG.info("Setting up the session manager.");
    init();
  }

  private void init() {
    createdSessions = Collections.synchronizedSet(new HashSet<SparkSession>());
    inited = true;
  }

  /**
   * If the <i>existingSession</i> can be reused return it.
   * Otherwise
   *   - close it and remove it from the list.
   *   - create a new session and add it to the list.
   */
  @Override
  public SparkSession getSession(SparkSession existingSession, HiveConf conf,
      boolean doOpen) throws HiveException {
    if (!inited) {
      init();
    }

    if (existingSession != null) {
      if (canReuseSession(existingSession, conf)) {
        // Open the session if it is closed.
        if (!existingSession.isOpen() && doOpen) {
          existingSession.open(conf);
        }

        Preconditions.checkState(createdSessions.contains(existingSession));
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Existing session (%s) is reused.",
              existingSession.getSessionId()));
        }
        return existingSession;
      } else {
        // Close the session, as the client is holding onto a session that can't be used
        // by the client anymore.
        closeSession(existingSession);
      }
    }

    SparkSession sparkSession = new SparkSessionImpl();
    createdSessions.add(sparkSession);

    if (doOpen) {
      sparkSession.open(conf);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("New session (%s) is created.", sparkSession.getSessionId()));
    }
    return sparkSession;
  }

  /**
   * Currently we only match the userNames in existingSession conf and given conf.
   */
  private boolean canReuseSession(SparkSession existingSession, HiveConf conf) throws HiveException {
    try {
      UserGroupInformation newUgi = ShimLoader.getHadoopShims().getUGIForConf(conf);
      String newUserName = ShimLoader.getHadoopShims().getShortUserName(newUgi);

      UserGroupInformation ugiInSession =
          ShimLoader.getHadoopShims().getUGIForConf(existingSession.getConf());
      String userNameInSession = ShimLoader.getHadoopShims().getShortUserName(ugiInSession);

      return newUserName.equals(userNameInSession);
    } catch(Exception ex) {
      throw new HiveException("Failed to get user info from HiveConf.", ex);
    }
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
    if (createdSessions != null) {
      synchronized(createdSessions) {
        Iterator<SparkSession> it = createdSessions.iterator();
        while (it.hasNext()) {
          SparkSession session = it.next();
          session.close();
        }
        createdSessions.clear();
      }
    }
    inited = false;
  }
}
