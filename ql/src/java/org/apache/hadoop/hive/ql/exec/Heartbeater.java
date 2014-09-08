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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Class to handle heartbeats for MR and Tez tasks.
 */
public class Heartbeater {
  private long lastHeartbeat = 0;
  private long heartbeatInterval = 0;
  private boolean dontHeartbeat = false;
  private HiveTxnManager txnMgr;
  private Configuration conf;

  static final private Log LOG = LogFactory.getLog(Heartbeater.class.getName());

  /**
   *
   * @param txnMgr transaction manager for this operation
   * @param conf Configuration for this operation
   */
  public Heartbeater(HiveTxnManager txnMgr, Configuration conf) {
    this.txnMgr = txnMgr;
    this.conf = conf;
  }

  /**
   * Send a heartbeat to the metastore for locks and transactions.
   * @throws IOException
   */
  public void heartbeat() throws IOException {
    if (dontHeartbeat) return;

    if (txnMgr == null) {
      LOG.debug("txnMgr null, not heartbeating");
      dontHeartbeat = true;
      return;
    }

    if (heartbeatInterval == 0) {
      // Multiply the heartbeat interval by 1000 to convert to milliseconds,
      // but divide by 2 to give us a safety factor.
      heartbeatInterval = HiveConf.getTimeVar(
          conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2;
      if (heartbeatInterval == 0) {
        LOG.warn(HiveConf.ConfVars.HIVE_TXN_MANAGER.toString() + " not set, heartbeats won't be sent");
        dontHeartbeat = true;
        LOG.debug("heartbeat interval 0, not heartbeating");
        return;
      }
    }
    long now = System.currentTimeMillis();
    if (now - lastHeartbeat > heartbeatInterval) {
      try {
        LOG.debug("heartbeating");
        txnMgr.heartbeat();
      } catch (LockException e) {
        LOG.warn("Failed trying to heartbeat " + e.getMessage());
        throw new IOException(e);
      }
      lastHeartbeat = now;
    }
  }

}
