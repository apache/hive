/**
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
 */package org.apache.hadoop.hive.ql.exec.tez;

import org.slf4j.LoggerFactory;

import org.slf4j.Logger;

import java.util.Set;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.UserPoolMapping.MappingInput;

public class WorkloadManagerFederation {
  private static final Logger LOG = LoggerFactory.getLogger(WorkloadManagerFederation.class);

  public static TezSessionState getSession(TezSessionState session, HiveConf conf,
      MappingInput input, boolean isUnmanagedLlapMode, Set<String> desiredCounters) throws Exception {
    // 1. If WM is not present just go to unmanaged.
    WorkloadManager wm = WorkloadManager.getInstance();
    if (wm == null) {
      LOG.debug("Using unmanaged session - WM is not initialized");
      return getUnmanagedSession(session, conf, desiredCounters, isUnmanagedLlapMode);
    }
    // 2. We will ask WM for a preliminary mapping. This allows us to escape to the unmanaged path
    //    quickly in the common case. It's still possible that resource plan will be updated and
    //    our preliminary mapping won't work out. We'll handle that below.
    if (!wm.isManaged(input)) {
      LOG.info("Using unmanaged session - no mapping for " + input);
      return getUnmanagedSession(session, conf, desiredCounters, isUnmanagedLlapMode);
    }

    // 3. Finally, try WM.
    try {
      // Note: this may just block to wait for a session based on parallelism.
      LOG.info("Getting a WM session for " + input);
      TezSessionState result = wm.getSession(session, input, conf);
      desiredCounters.addAll(wm.getTriggerCounterNames(result));
      return result;
    } catch (WorkloadManager.NoPoolMappingException ex) {
      LOG.info("NoPoolMappingException thrown. Getting an un-managed session..");
      return getUnmanagedSession(session, conf, desiredCounters, isUnmanagedLlapMode);
    }
  }

  private static TezSessionState getUnmanagedSession(
      TezSessionState session, HiveConf conf, Set<String> desiredCounters, boolean isWorkLlapNode) throws Exception {
    TezSessionPoolManager pm = TezSessionPoolManager.getInstance();
    session = pm.getSession(session, conf, false, isWorkLlapNode);
    desiredCounters.addAll(pm.getTriggerCounterNames());
    return session;
  }

}
