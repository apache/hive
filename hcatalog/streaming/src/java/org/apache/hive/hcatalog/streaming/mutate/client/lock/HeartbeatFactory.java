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
package org.apache.hive.hcatalog.streaming.mutate.client.lock;

import java.util.Collection;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates a default {@link HeartbeatTimerTask} for {@link Lock Locks}. */
class HeartbeatFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatFactory.class);

  /** Creates a new {@link HeartbeatTimerTask} instance for the {@link Lock} and schedules it. */
  Timer newInstance(IMetaStoreClient metaStoreClient, LockFailureListener listener, Long transactionId,
      Collection<Table> tableDescriptors, long lockId, int heartbeatPeriod) {
    Timer heartbeatTimer = new Timer("hive-lock-heartbeat[lockId=" + lockId + ", transactionId=" + transactionId + "]",
        true);
    HeartbeatTimerTask task = new HeartbeatTimerTask(metaStoreClient, listener, transactionId, tableDescriptors, lockId);
    heartbeatTimer.schedule(task, TimeUnit.SECONDS.toMillis(heartbeatPeriod),
        TimeUnit.SECONDS.toMillis(heartbeatPeriod));

    LOG.debug("Scheduled heartbeat timer task: {}", heartbeatTimer);
    return heartbeatTimer;
  }

}