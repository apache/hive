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
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.io.Serializable;

/**
 * Atlas metadata replication load work.
 */
@Explain(displayName = "Atlas Meta Data Load Work", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
public class AtlasLoadWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String srcDB;
  private final String tgtDB;
  private final Path stagingDir;
  private final transient ReplicationMetricCollector metricCollector;

  public AtlasLoadWork(String srcDB, String tgtDB, Path stagingDir, ReplicationMetricCollector metricCollector) {
    this.srcDB = srcDB;
    this.tgtDB = tgtDB;
    this.stagingDir = stagingDir;
    this.metricCollector = metricCollector;
  }

  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  public String getSrcDB() {
    return srcDB;
  }

  public String getTgtDB() {
    return tgtDB;
  }

  public Path getStagingDir() {
    return stagingDir;
  }

  public ReplicationMetricCollector getMetricCollector() {
    return metricCollector;
  }
}
