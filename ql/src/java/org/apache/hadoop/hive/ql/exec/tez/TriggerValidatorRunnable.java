/**
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

import org.apache.hadoop.hive.ql.wm.SessionTriggerProvider;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.apache.hadoop.hive.ql.wm.TriggerContext;
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
      Map<TezSessionState, Trigger.Action> violatedSessions = new HashMap<>();
      final List<TezSessionState> sessions = sessionTriggerProvider.getOpenSessions();
      final List<Trigger> triggers = sessionTriggerProvider.getActiveTriggers();
      for (TezSessionState s : sessions) {
        TriggerContext triggerContext = s.getTriggerContext();
        if (triggerContext != null) {
          Map<String, Long> currentCounters = triggerContext.getCurrentCounters();
          for (Trigger t : triggers) {
            String desiredCounter = t.getExpression().getCounterLimit().getName();
            // there could be interval where desired counter value is not populated by the time we make this check
            if (currentCounters.containsKey(desiredCounter)) {
              if (t.apply(currentCounters.get(desiredCounter))) {
                String queryId = s.getTriggerContext().getQueryId();
                LOG.info("Query {} violated trigger {}. Going to apply action {}", queryId, t, t.getAction());
                violatedSessions.put(s, t.getAction());
              }
            }
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
