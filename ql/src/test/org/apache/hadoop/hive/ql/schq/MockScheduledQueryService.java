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
package org.apache.hadoop.hive.ql.schq;

import org.apache.hadoop.hive.metastore.api.QueryState;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.ql.scheduled.IScheduledQueryMaintenanceService;

public class MockScheduledQueryService implements IScheduledQueryMaintenanceService {
    // Use notify/wait on this object to indicate when the scheduled query has finished executing.
    public final Object notifier = new Object();

    int id = 0;
    private String stmt;
    public ScheduledQueryProgressInfo lastProgressInfo;

    public MockScheduledQueryService(String string) {
        stmt = string;
    }

    @Override
    public ScheduledQueryPollResponse scheduledQueryPoll() {
        ScheduledQueryPollResponse r = new ScheduledQueryPollResponse();
        r.setQuery(stmt);
        r.setScheduleKey(new ScheduledQueryKey("sch1", getClusterNamespace()));
        r.setUser("nobody");
        if (id == 0) {
            r.setExecutionId(id++);
            return r;
        } else {
            return r;
        }
    }

    @Override
    public void scheduledQueryProgress(ScheduledQueryProgressInfo info) {
        System.out.printf("%d, state: %s, error: %s", info.getScheduledExecutionId(), info.getState(),
                info.getErrorMessage());
        lastProgressInfo = info;
        if (info.getState() == QueryState.FINISHED || info.getState() == QueryState.FAILED) {
            // Query is done, notify any waiters
            synchronized (notifier) {
                notifier.notifyAll();
            }
        }
    }

    @Override
    public String getClusterNamespace() {
        return "default";
    }
}