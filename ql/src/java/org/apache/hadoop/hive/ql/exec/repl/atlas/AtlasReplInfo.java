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

package org.apache.hadoop.hive.ql.exec.repl.atlas;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Atlas metadata replication info holder.
 */
public class AtlasReplInfo {
  private final String srcDB;
  private final String tgtDB;
  private final String srcCluster;
  private final String tgtCluster;
  private final Path stagingDir;
  private final HiveConf conf;
  private final String atlasEndpoint;
  private String srcFsUri;
  private String tgtFsUri;
  private long timeStamp;
  private Path tableListFile;

  public AtlasReplInfo(String atlasEndpoint, String srcDB, String tgtDB, String srcCluster,
                       String tgtCluster, Path stagingDir, HiveConf conf) {
    this(atlasEndpoint, srcDB, tgtDB, srcCluster, tgtCluster, stagingDir, null, conf);
  }

  public AtlasReplInfo(String atlasEndpoint, String srcDB, String tgtDB, String srcCluster,
                       String tgtCluster, Path stagingDir, Path tableListFile, HiveConf conf) {
    this.atlasEndpoint = atlasEndpoint;
    this.srcDB = srcDB;
    this.tgtDB = tgtDB;
    this.srcCluster = srcCluster;
    this.tgtCluster = tgtCluster;
    this.stagingDir = stagingDir;
    this.conf = conf;
    this.tableListFile = tableListFile;
  }

  public String getSrcDB() {
    return srcDB;
  }

  public String getTgtDB() {
    return tgtDB;
  }

  public String getSrcCluster() {
    return srcCluster;
  }

  public String getTgtCluster() {
    return tgtCluster;
  }

  public Path getStagingDir() {
    return stagingDir;
  }

  public HiveConf getConf() {
    return conf;
  }

  public String getAtlasEndpoint() {
    return atlasEndpoint;
  }

  public String getSrcFsUri() {
    return srcFsUri;
  }

  public void setSrcFsUri(String srcFsUri) {
    this.srcFsUri = srcFsUri;
  }

  public String getTgtFsUri() {
    return tgtFsUri;
  }

  public void setTgtFsUri(String tgtFsUri) {
    this.tgtFsUri = tgtFsUri;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public Path getTableListFile() {
    return tableListFile;
  }

  public void setTableListFile(Path tableListFile) {
    this.tableListFile = tableListFile;
  }

  public boolean isTableLevelRepl() {
    return this.tableListFile != null;
  }
}
