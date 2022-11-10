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
package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hadoop.hive.ql.wm.WmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Workload Manager events at query level.
 */
@JsonSerialize
public class WmEvent {
  private static final Logger LOG = LoggerFactory.getLogger(WmEvent.class);
  enum EventType {
    GET, // get session
    UPDATE, // update session allocation
    KILL, // kill query
    DESTROY, // destroy session
    RESTART, // restart session
    RETURN, // return session back to pool
    MOVE // move session to different pool
  }

  // snapshot of subset of wm tez session info for printing in events summary
  @JsonSerialize
  public static class WmTezSessionInfo {
    @JsonProperty("sessionId")
    private final String sessionId;
    @JsonProperty("poolName")
    private final String poolName;
    @JsonProperty("clusterPercent")
    private final double clusterPercent;

    WmTezSessionInfo(WmTezSession wmTezSession) {
      this.poolName = wmTezSession.getPoolName();
      this.sessionId = wmTezSession.getSessionId();
      this.clusterPercent = wmTezSession.hasClusterFraction()
          ? wmTezSession.getClusterFraction() * 100.0 : 0;
    }

    public String getPoolName() {
      return poolName;
    }

    public String getSessionId() {
      return sessionId;
    }

    public double getClusterPercent() {
      return clusterPercent;
    }

    @Override
    public String toString() {
      return "SessionId: " + sessionId + " Pool: " + poolName +  " Cluster %: " + clusterPercent;
    }
  }

  @JsonProperty("wmTezSessionInfo")
  private WmTezSessionInfo wmTezSessionInfo;
  @JsonProperty("eventStartTimestamp")
  private long eventStartTimestamp;
  @JsonProperty("eventEndTimestamp")
  private long eventEndTimestamp;
  @JsonProperty("eventType")
  private final EventType eventType;
  @JsonProperty("elapsedTime")
  private long elapsedTime;

  WmEvent(final EventType eventType) {
    this.eventType = eventType;
    this.eventStartTimestamp = System.currentTimeMillis();
  }

  public long getEventStartTimestamp() {
    return eventStartTimestamp;
  }

  public EventType getEventType() {
    return eventType;
  }

  public WmTezSessionInfo getWmTezSessionInfo() {
    return wmTezSessionInfo;
  }

  public long getEventEndTimestamp() {
    return eventEndTimestamp;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public void endEvent(final WmTezSession sessionState) {
    this.wmTezSessionInfo = new WmTezSessionInfo(sessionState);
    this.eventEndTimestamp = System.currentTimeMillis();
    this.elapsedTime = eventEndTimestamp - eventStartTimestamp;
    WmContext wmContext = sessionState.getWmContext();
    if (wmContext != null) {
      wmContext.addWMEvent(this);
      LOG.info("Added WMEvent: {}", this);
    }
  }

  @Override
  public String toString() {
    return "EventType: " + eventType + " EventStartTimestamp: " + eventStartTimestamp + " elapsedTime: " +
      elapsedTime + " wmTezSessionInfo:" + wmTezSessionInfo;
  }
}
