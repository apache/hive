/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io;

import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

public class PositionDeleteInfo {

  private static final String CONF_KEY_SPEC_ID = "hive.io.context.position.delete.spec.id";
  private static final String CONF_KEY_PART_HASH = "hive.io.context.position.delete.partition.hash";
  private static final String CONF_KEY_FILE_PATH = "hive.io.context.position.delete.file.path";
  private static final String CONF_KEY_ROW_POSITION = "hive.io.context.position.delete.row.position";
  private static final String CONF_KEY_PARTITION_PROJECTION = "hive.io.context.position.delete.partition.projection";
  private static final String CONF_KEY_ROW_ID = "hive.io.context.row.id";
  private static final String CONF_KEY_LAST_UPDATED_SEQ = "hive.io.context.last.updated.sequence.number";

  public static PositionDeleteInfo parseFromConf(Configuration conf) {
    int specId = conf.getInt(CONF_KEY_SPEC_ID, -1);
    long partHash = conf.getLong(CONF_KEY_PART_HASH, -1);
    String filePath = conf.get(CONF_KEY_FILE_PATH);
    long rowPos = conf.getLong(CONF_KEY_ROW_POSITION, -1);
    String partitionProjection = conf.get(CONF_KEY_PARTITION_PROJECTION);
    Long rowId = Optional.ofNullable(conf.get(CONF_KEY_ROW_ID)).map(Long::parseLong).orElse(null);
    Long lastUpdatedSequenceNumber =
        Optional.ofNullable(conf.get(CONF_KEY_LAST_UPDATED_SEQ)).map(Long::parseLong).orElse(null);
    return new PositionDeleteInfo(specId, partHash, filePath, rowPos, partitionProjection, rowId,
        lastUpdatedSequenceNumber);
  }

  public static void setIntoConf(Configuration conf, int specId, long partHash, String filePath, long filePos,
      String partitionProjection, long rowId, long lastUpdatedSequenceNumber) {
    conf.setInt(CONF_KEY_SPEC_ID, specId);
    conf.setLong(CONF_KEY_PART_HASH, partHash);
    conf.set(CONF_KEY_FILE_PATH, filePath);
    conf.setLong(CONF_KEY_ROW_POSITION, filePos);
    conf.set(CONF_KEY_PARTITION_PROJECTION, partitionProjection);
    conf.setLong(CONF_KEY_ROW_ID, rowId);
    conf.setLong(CONF_KEY_LAST_UPDATED_SEQ, lastUpdatedSequenceNumber);
  }

  private final int specId;
  private final long partitionHash;
  private final String filePath;
  private final long filePos;
  private final String partitionProjection;
  private final Long rowId;
  private final Long lastUpdatedSequenceNumber;

  public PositionDeleteInfo(int specId, long partitionHash, String filePath, long filePos, String partitionProjection,
      Long rowId, Long lastUpdatedSequenceNumber) {
    this.specId = specId;
    this.partitionHash = partitionHash;
    this.filePath = filePath;
    this.filePos = filePos;
    this.partitionProjection = partitionProjection;
    this.rowId = rowId;
    this.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
  }

  public Long getRowId() {
    return rowId;
  }

  public Long getLastUpdatedSequenceNumber() {
    return lastUpdatedSequenceNumber;
  }

  public int getSpecId() {
    return specId;
  }

  public long getPartitionHash() {
    return partitionHash;
  }

  public String getFilePath() {
    return filePath;
  }

  public long getFilePos() {
    return filePos;
  }

  public String getPartitionProjection() {
    return partitionProjection;
  }
}
