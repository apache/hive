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
 * Atlas metadata replication work.
 */
@Explain(displayName = "Atlas Meta Data Dump Work", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
public class AtlasDumpWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String srcDB;
  private final Path stagingDir;
  private final boolean bootstrap;
  private final Path prevAtlasDumpDir;
  private final transient ReplicationMetricCollector metricCollector;
  private final Path tableListPath;


  public AtlasDumpWork(String srcDB, Path stagingDir, boolean bootstrap, Path prevAtlasDumpDir, Path tableListPath,
                       ReplicationMetricCollector metricCollector) {
    this.srcDB = srcDB;
    this.stagingDir = stagingDir;
    this.bootstrap = bootstrap;
    this.prevAtlasDumpDir = prevAtlasDumpDir;
    this.tableListPath = tableListPath;
    this.metricCollector = metricCollector;
  }

  public boolean isBootstrap() {
    return bootstrap;
  }

  public Path getPrevAtlasDumpDir() {
    return prevAtlasDumpDir;
  }

  public String getSrcDB() {
    return srcDB;
  }

  public Path getStagingDir() {
    return stagingDir;
  }

  public Path getTableListPath() {
    return tableListPath;
  }

  public ReplicationMetricCollector getMetricCollector() {
    return metricCollector;
  }
}
