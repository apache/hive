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
package org.apache.hadoop.hive.metastore.txn.entities;

import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnType;

/**
 * Class to represent one row in the TXNS table.
 */
public class OpenTxn {

  private long txnId;
  private TxnStatus status;
  private TxnType type;
  private long startedTime;
  private long lastHeartBeatTime;
  private String user;
  private String host;

  public OpenTxn(long txnId, TxnStatus status, TxnType type) {
    this.txnId = txnId;
    this.status = status;
    this.type = type;
  }

  public TxnInfo toTxnInfo() {
    TxnInfo info = new TxnInfo(getTxnId(), getStatus().toTxnState(), getUser(), getHost());
    info.setStartedTime(getStartedTime());
    info.setLastHeartbeatTime(getLastHeartBeatTime());
    return info;
  }

  public long getTxnId() {
    return txnId;
  }

  public void setTxnId(long txnId) {
    this.txnId = txnId;
  }

  public TxnStatus getStatus() {
    return status;
  }

  public void setStatus(TxnStatus status) {
    this.status = status;
  }

  public TxnType getType() {
    return type;
  }

  public void setType(TxnType type) {
    this.type = type;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public void setStartedTime(long startedTime) {
    this.startedTime = startedTime;
  }

  public long getLastHeartBeatTime() {
    return lastHeartBeatTime;
  }

  public void setLastHeartBeatTime(long lastHeartBeatTime) {
    this.lastHeartBeatTime = lastHeartBeatTime;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }
}
