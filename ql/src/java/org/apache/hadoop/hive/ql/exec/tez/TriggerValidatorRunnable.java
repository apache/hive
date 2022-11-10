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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.wm.Action;
import org.apache.hadoop.hive.ql.wm.SessionTriggerProvider;
import org.apache.hadoop.hive.ql.wm.TimeCounterLimit;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerValidatorRunnable implements Runnable {
  protected static transient Logger LOG = LoggerFactory.getLogger(TriggerValidatorRunnable.class);
  private final SessionTriggerProvider sessionTriggerProvider;
  private final TriggerActionHandler triggerActionHandler;

  TriggerValidatorRunnable(final SessionTriggerProvider sessionTriggerProvider,
    final TriggerActionHandler triggerActionHandler) {
    this.sessionTriggerProvider = sessionTriggerProvider;
    this.triggerActionHandler = triggerActionHandler;
  }

  @Override
  public void run() {
    try {
      Map<TezSessionState, Trigger> violatedSessions = new HashMap<>();
      final List<TezSessionState> sessions = sessionTriggerProvider.getSessions();
      final List<Trigger> triggers = sessionTriggerProvider.getTriggers();
      for (TezSessionState sessionState : sessions) {
        WmContext wmContext = sessionState.getWmContext();
        if (wmContext != null && !wmContext.isQueryCompleted()
          && !wmContext.getSubscribedCounters().isEmpty()) {
          Map<String, Long> currentCounters = wmContext.getCurrentCounters();
          wmContext.updateElapsedTimeCounter();
          for (Trigger currentTrigger : triggers) {
            String desiredCounter = currentTrigger.getExpression().getCounterLimit().getName();
            // there could be interval where desired counter value is not populated by the time we make this check
            LOG.debug("Validating trigger: {} against currentCounters: {}", currentTrigger, currentCounters);
            if (currentCounters.containsKey(desiredCounter)) {
              long currentCounterValue = currentCounters.get(desiredCounter);
              if (currentTrigger.apply(currentCounterValue)) {
                String queryId = sessionState.getWmContext().getQueryId();
                if (violatedSessions.containsKey(sessionState)) {
                  // session already has a violation
                  Trigger existingTrigger = violatedSessions.get(sessionState);
                  // KILL always takes priority over MOVE
                  if (existingTrigger.getAction().getType().equals(Action.Type.MOVE_TO_POOL) &&
                    currentTrigger.getAction().getType().equals(Action.Type.KILL_QUERY)) {
                    currentTrigger.setViolationMsg("Trigger " + currentTrigger + " violated. Current value: " +
                      currentCounterValue);
                    violatedSessions.put(sessionState, currentTrigger);
                    LOG.info("KILL trigger replacing MOVE for query {}", queryId);
                  } else if (existingTrigger.getAction().getType().equals(Action.Type.MOVE_TO_POOL) &&
                    currentTrigger.getAction().getType().equals(Action.Type.MOVE_TO_POOL)){
                    // if multiple MOVE happens, only first move will be chosen
                    LOG.warn("Conflicting MOVE triggers ({} and {}). Choosing the first MOVE trigger: {}",
                      existingTrigger, currentTrigger, existingTrigger.getName());
                  } else if (existingTrigger.getAction().getType().equals(Action.Type.KILL_QUERY) &&
                    currentTrigger.getAction().getType().equals(Action.Type.KILL_QUERY)){
                    // if multiple KILL happens, only first kill will be chosen
                    LOG.warn("Conflicting KILL triggers ({} and {}). Choosing the first KILL trigger: {}",
                      existingTrigger, currentTrigger, existingTrigger.getName());
                  }
                } else {
                  // first violation for the session
                  currentTrigger.setViolationMsg("Trigger " + currentTrigger + " violated. Current value: " +
                    currentCounterValue);
                  violatedSessions.put(sessionState, currentTrigger);
                }
              }
            }
          }

          Trigger chosenTrigger = violatedSessions.get(sessionState);
          if (chosenTrigger != null) {
            LOG.info("Query: {}. {}. Applying action.", sessionState.getWmContext().getQueryId(),
              chosenTrigger.getViolationMsg());
          }
        }
      }

      if (!violatedSessions.isEmpty()) {
        triggerActionHandler.applyAction(violatedSessions);
      }
    } catch (Throwable t) {
      // if exception is thrown in scheduled tasks, no further tasks will be scheduled, hence this ugly catch
      LOG.warn(TriggerValidatorRunnable.class.getSimpleName() + " caught exception.", t);
    }
  }
}
