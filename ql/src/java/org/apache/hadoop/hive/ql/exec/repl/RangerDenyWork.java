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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URL;

/**
 * RangerDenyWork.
 *
 * Work to create Ranger deny policy.
 **/
@Explain(displayName = "Ranger Deny Policy Operator", explainLevels = { Explain.Level.USER,
        Explain.Level.DEFAULT,
        Explain.Level.EXTENDED })
public class RangerDenyWork implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RangerDenyWork.class);
    private Path currentDumpPath;
    private String targetDbName;
    private String sourceDbName;
    private final transient ReplicationMetricCollector metricCollector;

    public RangerDenyWork(Path currentDumpPath, String sourceDbName, String targetDbName,
                          ReplicationMetricCollector metricCollector) {
        this.currentDumpPath = currentDumpPath;
        this.targetDbName = targetDbName;
        this.sourceDbName = sourceDbName;
        this.metricCollector = metricCollector;
    }

    public Path getCurrentDumpPath() {
        return currentDumpPath;
    }

    public String getTargetDbName() {
        return targetDbName;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    URL getRangerConfigResource() {
        return getClass().getClassLoader().getResource(ReplUtils.RANGER_CONFIGURATION_RESOURCE_NAME);
    }

    ReplicationMetricCollector getMetricCollector() {
        return metricCollector;
    }
}
