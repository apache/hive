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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;

import java.util.List;
import java.util.Map;

/**
 * UpdateTableColumnStatEvent
 * Event generated for table column stat update event.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UpdateTableColumnStatEvent extends ListenerEvent {
  private ColumnStatistics colStats;
  private String validWriteIds;
  private long writeId;
  private Map<String, String> parameters;

  /**
   * @param colStats Columns statistics Info.
   * @param parameters table parameters to be updated after stats are updated.
   * @param validWriteIds valid write id list for the query.
   * @param colStats writeId for the query.
   * @param handler handler that is firing the event
   */
  public UpdateTableColumnStatEvent(ColumnStatistics colStats, Map<String, String> parameters, String validWriteIds,
                                    long writeId, IHMSHandler handler) {
    super(true, handler);
    this.colStats = colStats;
    this.validWriteIds = validWriteIds;
    this.writeId = writeId;
    this.parameters = parameters;
  }

  /**
   * @param colStats Columns statistics Info.
   * @param handler handler that is firing the event
   */
  public UpdateTableColumnStatEvent(ColumnStatistics colStats, IHMSHandler handler) {
    super(true, handler);
    this.colStats = colStats;
    this.validWriteIds = null;
    this.writeId = 0;
    this.parameters = null;
  }

  public ColumnStatistics getColStats() {
    return colStats;
  }

  public String getValidWriteIds() {
    return validWriteIds;
  }

  public long getWriteId() {
    return writeId;
  }

  public Map<String, String> getTableParameters() {
    return parameters;
  }
}
