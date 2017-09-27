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

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.registry.impl.TezAmInstance;

public class WmTezSession extends TezSessionPoolSession implements AmPluginNode {
  private String poolName;
  private double clusterFraction;

  private final Object amPluginInfoLock = new Object();
  private AmPluginInfo amPluginInfo = null;


  /** The actual state of the guaranteed task, and the update state, for the session. */
  // Note: hypothetically, a generic WM-aware-session should not know about guaranteed tasks.
  //       We should have another subclass for a WM-aware-session-implemented-using-ducks.
  //       However, since this is the only type of WM for now, this can live here.
  private final static class ActualWmState {
    // All accesses synchronized on the object itself. Could be replaced with CAS.
    int sending = -1, sent = -1, target = 0;
  }
  private final ActualWmState actualState = new ActualWmState();

  public WmTezSession(String sessionId, Manager parent,
      SessionExpirationTracker expiration, HiveConf conf) {
    super(sessionId, parent, expiration, conf);
  }

  @Override
  public AmPluginInfo waitForAmPluginInfo(int timeoutMs)
      throws InterruptedException, TimeoutException {
    synchronized (amPluginInfoLock) {
      if (amPluginInfo == null) {
        amPluginInfoLock.wait(timeoutMs);
        if (amPluginInfo == null) {
          throw new TimeoutException("No plugin information for " + getSessionId());
        }
      }
      return amPluginInfo;
    }
  }

  @Override
  void updateFromRegistry(TezAmInstance si) {
    synchronized (amPluginInfoLock) {
      this.amPluginInfo = new AmPluginInfo(si.getHost(), si.getPluginPort(),
          si.getPluginToken(), si.getPluginTokenJobId());
      amPluginInfoLock.notifyAll();
    }
  }

  public AmPluginInfo getAmPluginInfo() {
    return amPluginInfo; // Only has final fields, no artifacts from the absence of sync.
  }

  void setPoolName(String poolName) {
    this.poolName = poolName;
  }

  String getPoolName() {
    return poolName;
  }

  void setClusterFraction(double fraction) {
    this.clusterFraction = fraction;
  }

  double getClusterFraction() {
    return this.clusterFraction;
  }

  boolean setSendingGuaranteed(int intAlloc) {
    assert intAlloc >= 0;
    synchronized (actualState) {
      actualState.target = intAlloc;
      if (actualState.sending != -1) return false; // The sender will take care of this.
      if (actualState.sent == intAlloc) return false; // The value didn't change.
      actualState.sending = intAlloc;
      return true;
    }
  }

  int setSentGuaranteed() {
    // Only one send can be active at the same time.
    synchronized (actualState) {
      assert actualState.sending != -1;
      actualState.sent = actualState.sending;
      actualState.sending = -1;
      return (actualState.sent == actualState.target) ? -1 : actualState.target;
    }
  }

  boolean setFailedToSendGuaranteed() {
    synchronized (actualState) {
      assert actualState.sending != -1;
      actualState.sending = -1;
      // It's ok to skip a failed message if the target has changed back to the old value.
      return (actualState.sent == actualState.target);
    }
  }
}
