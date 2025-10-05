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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.wm.Action;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hadoop.hive.ql.wm.TriggerActionHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles only Kill Action.
 */
public class KillTriggerActionHandler implements TriggerActionHandler<TezSessionState> {
  private static final Logger LOG = LoggerFactory.getLogger(KillTriggerActionHandler.class);
  private final HiveConf conf;

  public KillTriggerActionHandler() {
      this.conf = new HiveConf();
  }

  @Override
  public void applyAction(final Map<TezSessionState, Trigger> queriesViolated) {
    for (Map.Entry<TezSessionState, Trigger> entry : queriesViolated.entrySet()) {
        if (entry.getValue().getAction().getType() == Action.Type.KILL_QUERY) {
            TezSessionState sessionState = entry.getKey();
            String queryId = sessionState.getWmContext().getQueryId();
            try {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                DriverUtils.setUpAndStartSessionState(conf, ugi.getShortUserName());
                KillQuery killQuery = sessionState.getKillQuery();
                // if kill query is null then session might have been released to pool or closed already
                if (killQuery != null) {
                    sessionState.getKillQuery().killQuery(queryId, entry.getValue().getViolationMsg(),
                            sessionState.getConf());
                }
            } catch (HiveException | IOException e) {
                LOG.warn("Unable to kill query {} for trigger violation", queryId);
            }
        } else {
            throw new RuntimeException("Unsupported action: " + entry.getValue());
        }
    }
  }
}
