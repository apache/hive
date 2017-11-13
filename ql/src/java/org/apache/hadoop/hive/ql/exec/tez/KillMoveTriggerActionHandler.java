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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KillMoveTriggerActionHandler implements TriggerActionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KillMoveTriggerActionHandler.class);
  private final WorkloadManager wm;

  KillMoveTriggerActionHandler(final WorkloadManager wm) {
    this.wm = wm;
  }

  @Override
  public void applyAction(final Map<TezSessionState, Trigger> queriesViolated) {
    TezSessionState sessionState;
    Map<WmTezSession, Future<Boolean>> moveFutures = new HashMap<>(queriesViolated.size());
    for (Map.Entry<TezSessionState, Trigger> entry : queriesViolated.entrySet()) {
      switch (entry.getValue().getAction().getType()) {
        case KILL_QUERY:
          sessionState = entry.getKey();
          String queryId = sessionState.getTriggerContext().getQueryId();
          try {
            sessionState.getKillQuery().killQuery(queryId, entry.getValue().getViolationMsg());
          } catch (HiveException e) {
            LOG.warn("Unable to kill query {} for trigger violation");
          }
          break;
        case MOVE_TO_POOL:
          sessionState = entry.getKey();
          if (sessionState instanceof WmTezSession) {
            WmTezSession wmTezSession = (WmTezSession) sessionState;
            String destPoolName = entry.getValue().getAction().getPoolName();
            Future<Boolean> moveFuture = wm.applyMoveSessionAsync(wmTezSession, destPoolName);
            moveFutures.put(wmTezSession, moveFuture);
          } else {
            throw new RuntimeException("WmTezSession is expected. Got: " + sessionState.getClass().getSimpleName() +
              ". SessionId: " + sessionState.getSessionId());
          }
          break;
        default:
          throw new RuntimeException("Unsupported action: " + entry.getValue());
      }
    }

    for (Map.Entry<WmTezSession, Future<Boolean>> entry : moveFutures.entrySet()) {
      WmTezSession wmTezSession = entry.getKey();
      Future<Boolean> moveFuture = entry.getValue();
      try {
        // block to make sure move happened successfully
        if (moveFuture.get()) {
          LOG.info("Moved session {} to pool {}", wmTezSession.getSessionId(), wmTezSession.getPoolName());
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Exception while moving session {}", wmTezSession.getSessionId(), e);
      }
    }
  }
}
