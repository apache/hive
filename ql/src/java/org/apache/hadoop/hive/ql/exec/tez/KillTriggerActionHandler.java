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

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles only Kill Action.
 */
public class KillTriggerActionHandler implements TriggerActionHandler<TezSessionState> {
  private static final Logger LOG = LoggerFactory.getLogger(KillTriggerActionHandler.class);

  @Override
  public void applyAction(final Map<TezSessionState, Trigger> queriesViolated) {
    for (Map.Entry<TezSessionState, Trigger> entry : queriesViolated.entrySet()) {
      switch (entry.getValue().getAction().getType()) {
        case KILL_QUERY:
          TezSessionState sessionState = entry.getKey();
          String queryId = sessionState.getWmContext().getQueryId();
          try {
            KillQuery killQuery = sessionState.getKillQuery();
            // if kill query is null then session might have been released to pool or closed already
            if (killQuery != null) {
              sessionState.getKillQuery().killQuery(queryId, entry.getValue().getViolationMsg(),
                      sessionState.getConf());
            }
          } catch (HiveException e) {
            LOG.warn("Unable to kill query {} for trigger violation");
          }
          break;
        default:
          throw new RuntimeException("Unsupported action: " + entry.getValue());
      }
    }
  }
}
