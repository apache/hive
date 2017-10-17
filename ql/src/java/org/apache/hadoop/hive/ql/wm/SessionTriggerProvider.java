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
package org.apache.hadoop.hive.ql.wm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.tez.TezSessionState;

/**
 * Implementation for providing current open sessions and active trigger.
 */
public class SessionTriggerProvider {
  private List<TezSessionState> openSessions = new ArrayList<>();
  private List<Trigger> activeTriggers = new ArrayList<>();

  public SessionTriggerProvider() {

  }

  public SessionTriggerProvider(final List<TezSessionState> openSessions, final List<Trigger> triggers) {
    this.openSessions = openSessions;
    this.activeTriggers = triggers;
  }

  public void setOpenSessions(final List<TezSessionState> openSessions) {
    this.openSessions = openSessions;
  }

  public void setActiveTriggers(final List<Trigger> activeTriggers) {
    this.activeTriggers = activeTriggers;
  }

  public List<TezSessionState> getOpenSessions() {
    return Collections.unmodifiableList(openSessions);
  }

  public List<Trigger> getActiveTriggers() {
    return Collections.unmodifiableList(activeTriggers);
  }
}
