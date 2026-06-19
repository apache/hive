/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KillMoveTriggerActionHandler implements TriggerActionHandler<WmTezSession> {
  private static final Logger LOG = LoggerFactory.getLogger(KillMoveTriggerActionHandler.class);
  private final WorkloadManager wm;

  KillMoveTriggerActionHandler(final WorkloadManager wm) {
    this.wm = wm;
  }

  @Override
  public void applyAction(final Map<WmTezSession, Trigger> queriesViolated) {
    Map<WmTezSession, Future<Boolean>> moveFutures = new HashMap<>();
    Map<WmTezSession, Future<Boolean>> killFutures = new HashMap<>();
    for (Map.Entry<WmTezSession, Trigger> entry : queriesViolated.entrySet()) {
      WmTezSession wmTezSession = entry.getKey();
      switch (entry.getValue().getAction().getType()) {
        case KILL_QUERY:
          Future<Boolean> killFuture = wm.applyKillSessionAsync(wmTezSession, entry.getValue().getViolationMsg());
          killFutures.put(wmTezSession, killFuture);
          break;
        case MOVE_TO_POOL:
          String destPoolName = entry.getValue().getAction().getPoolName();
          if (!wmTezSession.isDelayedMove()) {
            Future<Boolean> moveFuture = wm.applyMoveSessionAsync(wmTezSession, destPoolName);
            moveFutures.put(wmTezSession, moveFuture);
          }
          break;
        default:
          throw new RuntimeException("Unsupported action: " + entry.getValue());
      }
    }

    for (Map.Entry<WmTezSession, Future<Boolean>> entry : moveFutures.entrySet()) {
      WmTezSession wmTezSession = entry.getKey();
      Future<Boolean> future = entry.getValue();
      try {
        // block to make sure move happened successfully
        if (future.get()) {
          LOG.info("Moved session {} to pool {}", wmTezSession.getSessionId(), wmTezSession.getPoolName());
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Exception while moving session {}", wmTezSession.getSessionId(), e);
      }
    }

    for (Map.Entry<WmTezSession, Future<Boolean>> entry : killFutures.entrySet()) {
      WmTezSession wmTezSession = entry.getKey();
      Future<Boolean> future = entry.getValue();
      try {
        // block to make sure kill happened successfully
        if (future.get()) {
          LOG.info("Killed session {}", wmTezSession.getSessionId());
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Exception while killing session {}", wmTezSession.getSessionId(), e);
      }
    }
  }
}
