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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.conf.Configuration;

public class RowLineageInfo {

  private final Long baseRowId;
  private final Long lastUpdatedSequenceNumber;

  private static final String CONF_KEY_ROW_ID = "hive.io.context.row.lineage.row.id";
  private static final String CONF_KEY_LAST_UPDATED_SEQUENCE_NUMBER =
      "hive.io.context.row.lineage.last.updated.sequence.number";

  public RowLineageInfo(Long baseRowId, Long lastUpdatedSequenceNumber) {
    this.baseRowId = baseRowId;
    this.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
  }

  public Long getBaseRowId() {
    return baseRowId;
  }

  public Long getLastUpdatedSequenceNumber() {
    return lastUpdatedSequenceNumber;
  }

  public static RowLineageInfo parseFromConf(Configuration conf) {
    Long rowId = conf.get(CONF_KEY_ROW_ID) != null ? Long.parseLong(conf.get(CONF_KEY_ROW_ID)) : null;

    Long lusn = conf.get(CONF_KEY_LAST_UPDATED_SEQUENCE_NUMBER) != null ?
        Long.parseLong(conf.get(CONF_KEY_LAST_UPDATED_SEQUENCE_NUMBER)) :
        null;

    return new RowLineageInfo(rowId, lusn);
  }

  public static void setRowLineageInfoIntoConf(Long rowId, Long lusn, Configuration conf) {
    if (rowId != null) {
      conf.setLong(CONF_KEY_ROW_ID, rowId);
    }
    if (lusn != null) {
      conf.setLong(CONF_KEY_LAST_UPDATED_SEQUENCE_NUMBER, lusn);
    }
  }
}