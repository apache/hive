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
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.StringConvertibleObject;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.Explain;
import java.io.Serializable;

import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.SnapshotCopyMode.FALLBACK_COPY;

/**
 * DirCopyWork, mainly to be used to copy External table data.
 */
@Explain(displayName = "HDFS Copy Operator", explainLevels = { Explain.Level.USER,
        Explain.Level.DEFAULT,
        Explain.Level.EXTENDED })
public class DirCopyWork implements Serializable, StringConvertibleObject {
  public static final String URI_SEPARATOR = "#";
  private static final long serialVersionUID = 1L;
  private String tableName;
  private Path fullyQualifiedSourcePath;
  private Path fullyQualifiedTargetPath;
  private String dumpDirectory;
  private transient ReplicationMetricCollector metricCollector;
  private SnapshotUtils.SnapshotCopyMode copyMode = FALLBACK_COPY;
  private String snapshotPrefix = "";

  public DirCopyWork(ReplicationMetricCollector metricCollector, String dumpDirectory) {
    this.metricCollector = metricCollector;
    this.dumpDirectory = dumpDirectory;
  }

  public DirCopyWork(String tableName, Path fullyQualifiedSourcePath, Path fullyQualifiedTargetPath) {
    this.tableName = tableName;
    this.fullyQualifiedSourcePath = fullyQualifiedSourcePath;
    this.fullyQualifiedTargetPath = fullyQualifiedTargetPath;
  }

  public DirCopyWork(String tableName, Path fullyQualifiedSourcePath, Path fullyQualifiedTargetPath,
      SnapshotUtils.SnapshotCopyMode copyMode, String snapshotPrefix) {
    this(tableName, fullyQualifiedSourcePath, fullyQualifiedTargetPath);
    this.copyMode = copyMode;
    this.snapshotPrefix = snapshotPrefix;
  }


  @Override
  public String toString() {
    return "DirCopyWork{"
            + "fullyQualifiedSourcePath=" + getFullyQualifiedSourcePath()
            + ", fullyQualifiedTargetPath=" + getFullyQualifiedTargetPath()
            + ", tableName=" + tableName
            + ", copyMode="+ getCopyMode()
            + ", snapshotPrefix="+ getSnapshotPrefix()
            + '}';
  }

  public Path getFullyQualifiedSourcePath() {
    return fullyQualifiedSourcePath;
  }

  public Path getFullyQualifiedTargetPath() {
    return fullyQualifiedTargetPath;
  }

  public ReplicationMetricCollector getMetricCollector() {
    return metricCollector;
  }

  public String getDumpDirectory() {
    return dumpDirectory;
  }

  public SnapshotUtils.SnapshotCopyMode getCopyMode() {
    return copyMode;
  }

  public String getSnapshotPrefix() {
    return snapshotPrefix;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public String convertToString() {
    StringBuilder objInStr = new StringBuilder();
    objInStr.append(fullyQualifiedSourcePath)
            .append(URI_SEPARATOR)
            .append(fullyQualifiedTargetPath)
            .append(URI_SEPARATOR)
            .append(tableName)
            .append(URI_SEPARATOR)
            .append(copyMode)
            .append(URI_SEPARATOR)
            .append(snapshotPrefix);

    return objInStr.toString();
  }

  @Override
  public void loadFromString(String objectInStr) {
    String paths[] = objectInStr.split(URI_SEPARATOR);
    this.fullyQualifiedSourcePath = new Path(paths[0]);
    this.fullyQualifiedTargetPath = new Path(paths[1]);
    if (paths.length > 3) {
      this.tableName = paths[2];
      this.copyMode = SnapshotUtils.SnapshotCopyMode.valueOf(paths[3]);
      this.snapshotPrefix = paths[4];
    }
  }
}
