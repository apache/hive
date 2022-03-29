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

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;

public class PositionDeleteInfo {

  private static final String CONF_KEY = "hive.io.context.position.delete.info";
  private static final Gson GSON = new Gson();

  public static PositionDeleteInfo parseFromConf(Configuration conf) {
    String value = conf.get(CONF_KEY);
    return value == null ? null : GSON.fromJson(value, PositionDeleteInfo.class);
  }

  public static void serializeIntoConf(Configuration conf, PositionDeleteInfo pdi) {
    conf.set(CONF_KEY, pdi.toJson());
  }

  private final int specId;
  private final long partitionHash;
  private final String filePath;
  private final long filePos;

  public PositionDeleteInfo(int specId, long partitionHash, String filePath, long filePos) {
    this.specId = specId;
    this.partitionHash = partitionHash;
    this.filePath = filePath;
    this.filePos = filePos;
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

  public String toJson() {
    return GSON.toJson(this);
  }
}
